-- Per-VM state cleanup on snapshot removal.
--
-- The vm_runtime_snapshots row is the VM's durable existence: it is
-- created together with the VM's runnable_workloads row at instance
-- registration, and deleting it is the single end-of-life act.  Action
-- call requests, action call completions, and sleep requests are all
-- subordinate state of a live VM; the trigger below removes them the
-- moment their VM's snapshot goes, so no deletion call site can forget
-- them.
--
-- A deliberate trigger rather than ON DELETE CASCADE foreign keys: a
-- foreign key's referential check costs an index probe and a KEY SHARE
-- lock on the parent row for every child insert — and these child
-- tables are hot write paths racing the snapshot rewrite of every
-- persist interval — while this trigger fires only on the rare
-- terminal delete.  The flip side, also deliberate: child inserts are
-- unchecked, so a completion recorded for an already-deleted VM (a
-- worker resolving late) lingers as an inert orphan row invisible to
-- the demand-driven pollers, instead of being rejected at insert.
-- Statement-level with a transition table so a batch of snapshot
-- deletes costs one DELETE per child table.
--
-- runnable_workloads is not listed here: it keeps its ON DELETE
-- CASCADE foreign key — its insert path is the rare registration
-- moment, not a hot loop.

CREATE FUNCTION vm_runtime_snapshots_cleanup_on_delete() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM action_call_requests r
    USING deleted d
    WHERE r.vm_id = d.vm_id;

    DELETE FROM action_call_completions c
    USING deleted d
    WHERE c.vm_id = d.vm_id;

    DELETE FROM sleep_requests s
    USING deleted d
    WHERE s.vm_id = d.vm_id;

    RETURN NULL;
END;
$$;

CREATE TRIGGER vm_runtime_snapshots_cleanup_on_delete
AFTER DELETE ON vm_runtime_snapshots
REFERENCING OLD TABLE AS deleted
FOR EACH STATEMENT
EXECUTE FUNCTION vm_runtime_snapshots_cleanup_on_delete();
