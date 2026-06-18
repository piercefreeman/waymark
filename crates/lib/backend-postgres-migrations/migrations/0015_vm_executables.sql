-- Persist compiled VM executables (bytecode), deduplicated by (name, version).

CREATE TABLE vm_executables (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT NOT NULL,
    bytecode BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (name, version)
);
