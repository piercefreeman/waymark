-- Add priority column to workflow_instances for queue ordering
-- Higher priority values are processed first (default 0)

ALTER TABLE workflow_instances
ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

-- Add priority column to workflow_schedules for scheduled runs
ALTER TABLE workflow_schedules
ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

-- Add priority column to action_queue (denormalized from instance for efficient dispatch)
-- Priority is copied from the instance when actions are enqueued
ALTER TABLE action_queue
ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;

-- Update the dispatch index to include priority for efficient ordering
-- Actions with higher priority are dispatched first
DROP INDEX IF EXISTS idx_action_queue_dispatch;
CREATE INDEX idx_action_queue_dispatch
    ON action_queue(priority DESC, scheduled_at, action_seq)
    WHERE status = 'queued';
