-- Track when instances are claimed for start to avoid rescans/dup starts.

ALTER TABLE workflow_instances
ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_workflow_instances_unstarted
ON workflow_instances (priority DESC, created_at)
WHERE status = 'running'
  AND next_action_seq = 0
  AND started_at IS NULL;
