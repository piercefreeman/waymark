CREATE TABLE IF NOT EXISTS workflow_versions (
    id BIGSERIAL PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    dag_hash TEXT NOT NULL,
    dag_proto BYTEA NOT NULL,
    concurrent BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (workflow_name, dag_hash)
);
