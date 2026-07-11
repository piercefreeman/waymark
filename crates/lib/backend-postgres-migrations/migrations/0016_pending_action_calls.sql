-- Pending action calls: durable records of dispatched action calls,
-- kept from just before the dispatch until the resulting settlement has
-- been persisted with the VM state. On revive, still-pending records are
-- re-dispatched.

CREATE TABLE pending_action_calls (
    vm_id UUID NOT NULL REFERENCES vm_runtime_snapshots(vm_id) ON DELETE CASCADE,
    promise_state_id BIGINT NOT NULL,
    effect_number BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (vm_id, promise_state_id)
);
