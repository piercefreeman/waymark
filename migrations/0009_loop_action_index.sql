-- Migration to allow multiple actions for the same node (for loops)
-- The unique constraint should only apply to non-completed actions

-- Drop the old index
DROP INDEX IF EXISTS idx_daemon_action_instance_node;

-- Create new partial unique index that only applies to non-completed actions
-- This allows loops to queue new iterations after previous ones complete
CREATE UNIQUE INDEX idx_daemon_action_instance_node
    ON daemon_action_ledger (instance_id, workflow_node_id)
    WHERE workflow_node_id IS NOT NULL AND status != 'completed';
