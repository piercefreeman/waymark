-- Recreate node_payloads with generated ID for COPY-based bulk inserts
-- This removes the composite primary key constraint, allowing faster writes

DROP TABLE IF EXISTS node_payloads;

CREATE TABLE node_payloads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL,
    node_id TEXT NOT NULL,
    inputs BYTEA,
    result BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(id) ON DELETE CASCADE
);

-- For bulk loading payloads when recovering an instance
CREATE INDEX idx_node_payloads_instance ON node_payloads(instance_id);

-- For deduplication queries (get latest payload per node)
CREATE INDEX idx_node_payloads_instance_node ON node_payloads(instance_id, node_id, created_at DESC);
