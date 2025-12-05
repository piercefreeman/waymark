-- Add node_type column to action_queue for unified readiness model
--
-- This enables the action_queue to handle both external actions (dispatched to workers)
-- and internal barriers (aggregators, parallel joins) that are processed by the runner.
--
-- node_type values:
--   'action'  - External action dispatched to Python worker
--   'barrier' - Internal synchronization point (aggregator, parallel join)

ALTER TABLE action_queue ADD COLUMN node_type TEXT NOT NULL DEFAULT 'action';

-- Add check constraint for valid node types
ALTER TABLE action_queue ADD CONSTRAINT action_queue_node_type_check
    CHECK (node_type IN ('action', 'barrier'));

-- Update dispatch index to include node_type for efficient filtering
DROP INDEX IF EXISTS idx_action_queue_dispatch;
CREATE INDEX idx_action_queue_dispatch
    ON action_queue(status, scheduled_at, action_seq)
    WHERE status = 'queued';
