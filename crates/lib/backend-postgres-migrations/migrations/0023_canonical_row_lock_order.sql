-- Canonical row-lock order for the trigger-side multi-row writers.
--
-- Any two multi-row statements that take row locks on overlapping sets
-- in different orders can deadlock; the 2026-08-31 production outage
-- was the completion trigger's DELETE (transition-table order) against
-- the batched lock-renewal UPDATE (input-array order) on the same
-- action_call_requests rows.  The discipline that makes such cycles
-- impossible is a single canonical acquisition order for every
-- multi-row writer on the trigger-swept tables: lock the target rows
-- via ORDER BY primary key FOR UPDATE, then mutate only the locked
-- set.  Sorting is reliable here because locking happens above the
-- sort in the plan and the sort key is the immutable primary key.
--
-- This migration installs the discipline in the two trigger functions;
-- the statement-side writers get the same treatment in the backend's
-- SQL.  The lock pass and the DELETE must be ONE statement — the
-- locked-CTE form below: as two statements each would take its own
-- READ COMMITTED snapshot, and a row committed into the target set
-- between the snapshots would be deleted without ever passing through
-- the ordered lock queue, reopening the cycle.  In the single
-- statement a row committed after the snapshot is simply not seen; it
-- survives the sweep as the already-accepted late-write orphan (see
-- 0020).

CREATE OR REPLACE FUNCTION action_call_requests_remove_on_completion() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    WITH locked AS MATERIALIZED (
        SELECT r.vm_id, r.promise_state_id
        FROM action_call_requests r
        JOIN inserted i
            ON r.vm_id = i.vm_id AND r.promise_state_id = i.promise_state_id
        ORDER BY r.vm_id, r.promise_state_id
        FOR UPDATE OF r
    )
    DELETE FROM action_call_requests r
    USING locked l
    WHERE r.vm_id = l.vm_id AND r.promise_state_id = l.promise_state_id;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION vm_runtime_snapshots_cleanup_on_delete() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    WITH locked AS MATERIALIZED (
        SELECT r.vm_id, r.promise_state_id
        FROM action_call_requests r
        JOIN deleted d ON r.vm_id = d.vm_id
        ORDER BY r.vm_id, r.promise_state_id
        FOR UPDATE OF r
    )
    DELETE FROM action_call_requests r
    USING locked l
    WHERE r.vm_id = l.vm_id AND r.promise_state_id = l.promise_state_id;

    WITH locked AS MATERIALIZED (
        SELECT c.vm_id, c.promise_state_id
        FROM action_call_completions c
        JOIN deleted d ON c.vm_id = d.vm_id
        ORDER BY c.vm_id, c.promise_state_id
        FOR UPDATE OF c
    )
    DELETE FROM action_call_completions c
    USING locked l
    WHERE c.vm_id = l.vm_id AND c.promise_state_id = l.promise_state_id;

    WITH locked AS MATERIALIZED (
        SELECT s.vm_id, s.promise_state_id
        FROM sleep_requests s
        JOIN deleted d ON s.vm_id = d.vm_id
        ORDER BY s.vm_id, s.promise_state_id
        FOR UPDATE OF s
    )
    DELETE FROM sleep_requests s
    USING locked l
    WHERE s.vm_id = l.vm_id AND s.promise_state_id = l.promise_state_id;

    RETURN NULL;
END;
$$;
