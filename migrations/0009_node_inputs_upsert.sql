-- Add unique constraint to enable UPSERT for non-spread inbox writes.
-- For loops, this prevents unbounded inbox growth by overwriting values
-- instead of appending.
--
-- spread_index IS NULL covers non-spread writes (regular variables).
-- spread_index IS NOT NULL entries remain unique per index.

-- First, delete duplicates keeping only the latest entry per (instance_id, target_node_id, variable_name, spread_index)
DELETE FROM node_inputs a USING (
    SELECT id, ROW_NUMBER() OVER (
        PARTITION BY instance_id, target_node_id, variable_name, COALESCE(spread_index, -1)
        ORDER BY created_at DESC
    ) as rn
    FROM node_inputs
) b
WHERE a.id = b.id AND b.rn > 1;

-- Add the unique constraint (treating NULL spread_index as a single value via COALESCE in the index)
CREATE UNIQUE INDEX idx_node_inputs_upsert
ON node_inputs(instance_id, target_node_id, variable_name, COALESCE(spread_index, -1));
