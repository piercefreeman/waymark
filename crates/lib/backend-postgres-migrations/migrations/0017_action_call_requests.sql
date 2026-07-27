-- Durably-stored action-call requests.
--
-- A row is recorded (born locked by the storing process) when the VM
-- emits an action-call effect, locked for delivery at VM revival
-- reconcile, and kept alive by lock renewal while the attempt runs in
-- the lock owner's local worker pool.  A re-emitted effect reuses the
-- same (vm_id, promise_state_id) pair, so the primary key deduplicates
-- replays.
--
-- There is deliberately no delete operation: rows are removed by the
-- trigger below the moment their completion is durably recorded, so the
-- existence of a row means the call's outcome is not durably known yet.

CREATE TABLE action_call_requests (
    vm_id UUID NOT NULL,
    promise_state_id BIGINT NOT NULL,
    effect_number BIGINT NOT NULL,
    request BYTEA NOT NULL,
    locked_by UUID,
    lock_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (vm_id, promise_state_id),
    -- A lock's owner and its expiry are set and cleared together: locked
    -- rows have both, unlocked rows have neither.  A row with only one
    -- set would match neither the delivery-eligibility nor the renewal
    -- predicate and become permanently undeliverable.
    CONSTRAINT action_call_requests_lock_owner_expiry_paired
        CHECK ((locked_by IS NULL) = (lock_expires_at IS NULL))
);

-- The removal invariant: recording a completion atomically removes the
-- matching pending request.  Owned by the requests subsystem, riding on
-- inserts into action_call_completions — reviewers of the completions
-- write path: this side effect is intentional.  Statement-level with a
-- transition table so a batch insert costs one DELETE.  Rows skipped by
-- ON CONFLICT DO NOTHING do not appear in the transition table, which is
-- correct: the first recording already removed the request row.
CREATE FUNCTION action_call_requests_remove_on_completion() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM action_call_requests r
    USING inserted i
    WHERE r.vm_id = i.vm_id AND r.promise_state_id = i.promise_state_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER action_call_requests_remove_on_completion
AFTER INSERT ON action_call_completions
REFERENCING NEW TABLE AS inserted
FOR EACH STATEMENT
EXECUTE FUNCTION action_call_requests_remove_on_completion();
