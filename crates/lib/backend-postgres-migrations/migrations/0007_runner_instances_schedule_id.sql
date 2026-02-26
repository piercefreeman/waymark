ALTER TABLE runner_instances
ADD COLUMN IF NOT EXISTS schedule_id UUID;

CREATE INDEX IF NOT EXISTS idx_runner_instances_schedule_id_created_at
    ON runner_instances(schedule_id, created_at DESC);
