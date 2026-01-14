-- Add index optimized for latest spread values per target node.
-- Supports DISTINCT ON (target_node_id, spread_index) with created_at DESC.
CREATE INDEX IF NOT EXISTS idx_node_inputs_spread_latest
ON node_inputs(instance_id, target_node_id, spread_index, created_at DESC)
WHERE spread_index IS NOT NULL;
