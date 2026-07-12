-- Durably-stored action-call completions.
--
-- A row is recorded when the worker pool reports an action completion and
-- removed (acked) once the resulting promise settlement has been applied
-- and the VM state persisted.  Rows of a terminally-completed VM are
-- purged wholesale.  A re-emitted effect reuses the same
-- (vm_id, promise_state_id) pair, so the primary key deduplicates
-- re-deliveries.

CREATE TABLE action_call_completions (
    vm_id UUID NOT NULL,
    promise_state_id BIGINT NOT NULL,
    effect_number BIGINT NOT NULL,
    outcome BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (vm_id, promise_state_id)
);
