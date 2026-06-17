-- Persist VM execution completion results (successful or exceptional).

CREATE TABLE vm_execution_results (
    vm_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    result BYTEA,
    error BYTEA,
    CONSTRAINT vm_execution_results_xor CHECK (
        (result IS NOT NULL AND error IS NULL)
        OR (result IS NULL AND error IS NOT NULL)
    )
);
