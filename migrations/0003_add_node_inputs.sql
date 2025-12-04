-- Node inputs table (inbox pattern)
-- Append-only ledger for passing values between nodes.
-- When Node A completes, it INSERTs into this table for each downstream target.
-- When Node B runs, it SELECTs all rows WHERE target_node_id = B.
-- This is O(1) writes (no locks!) and O(n) read when needed.

CREATE TABLE node_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    -- Target node that will receive this value
    target_node_id TEXT NOT NULL,
    -- Variable name being passed
    variable_name TEXT NOT NULL,
    -- JSON-encoded value
    value JSONB NOT NULL,
    -- Source node that produced this value (for debugging/tracing)
    source_node_id TEXT NOT NULL,
    -- For spread/parallel results - index for ordering
    spread_index INTEGER,
    -- Timestamp for ordering (append-only)
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for reading inbox: SELECT * FROM node_inputs WHERE instance_id = ? AND target_node_id = ?
CREATE INDEX idx_node_inputs_target ON node_inputs(instance_id, target_node_id);

-- Index for cleanup: DELETE FROM node_inputs WHERE instance_id = ?
CREATE INDEX idx_node_inputs_instance ON node_inputs(instance_id);
