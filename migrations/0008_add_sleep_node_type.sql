-- Add 'sleep' to valid node_type values
--
-- This enables durable sleep nodes that are scheduled for future execution.
-- Sleep actions have a scheduled_at time in the future and are processed
-- by the runner when that time arrives.

-- Drop the existing constraint
ALTER TABLE action_queue DROP CONSTRAINT IF EXISTS action_queue_node_type_check;

-- Add updated constraint with sleep
ALTER TABLE action_queue ADD CONSTRAINT action_queue_node_type_check
    CHECK (node_type IN ('action', 'barrier', 'for_loop', 'sleep'));
