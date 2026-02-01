-- Node payloads table for storing inputs/results separately from execution_graph
-- This allows the execution_graph to stay small for frequent updates while
-- payloads are written once (INSERT-only).

CREATE TABLE node_payloads (
    instance_id UUID NOT NULL,
    node_id TEXT NOT NULL,
    inputs BYTEA,
    result BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (instance_id, node_id),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(id) ON DELETE CASCADE
);

-- For bulk loading payloads when recovering an instance
CREATE INDEX idx_node_payloads_instance ON node_payloads(instance_id);
