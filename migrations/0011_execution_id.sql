-- Add execution_id column to node_payloads for unique per-execution identification
-- This replaces the node_id based lookup with a direct UUID key

ALTER TABLE node_payloads ADD COLUMN execution_id TEXT;

-- Index for fast lookup by execution_id
CREATE INDEX idx_node_payloads_execution_id ON node_payloads(execution_id) WHERE execution_id IS NOT NULL;

-- Note: node_id is kept for debugging/reference, but execution_id is now the primary lookup key
