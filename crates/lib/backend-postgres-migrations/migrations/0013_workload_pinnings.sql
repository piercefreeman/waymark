-- Workload pinning: tracks which node owns VM runtime instances.
-- Every row in vm_runtime_snapshots has a matching row here.

CREATE TABLE workload_pinnings (
    instance_id UUID PRIMARY KEY REFERENCES vm_runtime_snapshots(vm_id) ON DELETE CASCADE,
    node_id UUID,
    expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
