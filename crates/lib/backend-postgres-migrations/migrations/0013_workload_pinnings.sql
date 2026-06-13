-- Workload pinning: tracks which node owns VM runtime instances.
-- Every row in vm_runtime_snapshots has a matching row here.

CREATE TABLE workload_pinnings (
    instance_id UUID PRIMARY KEY REFERENCES vm_runtime_snapshots(vm_id) ON DELETE CASCADE,
    node_id UUID,
    expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- A pinning's owner and its lease expiry are set and cleared together:
    -- claimed rows have both, unclaimed rows have neither. A row with only one
    -- set would match neither the poll nor the refresh predicate and become
    -- permanently unclaimable.
    CONSTRAINT workload_pinnings_node_expiry_paired
        CHECK ((node_id IS NULL) = (expires_at IS NULL))
);
