-- Durably-recorded sleep requests.
--
-- A row is recorded when the VM emits a sleep effect and removed (acked)
-- once the resulting promise settlement has been applied and the VM
-- state persisted — the ack itself is the removal, so this single table
-- backs the whole flow.  Rows of a terminally-completed VM are purged
-- wholesale.  A re-emitted effect reuses the same
-- (vm_id, promise_state_id) pair, so the primary key deduplicates
-- replays and the originally recorded wake_at stands.

CREATE TABLE sleep_requests (
    vm_id UUID NOT NULL,
    promise_state_id BIGINT NOT NULL,
    effect_number BIGINT NOT NULL,
    wake_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (vm_id, promise_state_id)
);
