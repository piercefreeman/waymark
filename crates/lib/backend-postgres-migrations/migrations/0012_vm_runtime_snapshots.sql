-- VM runtime snapshot persistence.

CREATE TABLE vm_runtime_snapshots (
    vm_id UUID PRIMARY KEY,
    executable_id UUID NOT NULL,
    snapshot BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
