-- Convert node_inputs to append-only pattern for better throughput.
-- Remove UPSERT constraint in favor of simple INSERTs.
-- Reads will use DISTINCT ON to get latest values.

-- Drop the UPSERT unique constraint
DROP INDEX IF EXISTS idx_node_inputs_upsert;

-- Add index optimized for DISTINCT ON queries that fetch latest value per variable
-- This supports: ORDER BY variable_name, created_at DESC
CREATE INDEX idx_node_inputs_latest
ON node_inputs(instance_id, target_node_id, variable_name, created_at DESC);
