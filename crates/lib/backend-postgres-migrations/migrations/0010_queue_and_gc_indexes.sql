-- Optimize the hottest queue claim/reclaim paths and finished-instance GC.

CREATE INDEX IF NOT EXISTS idx_queued_instances_unlocked_scheduled_created
    ON queued_instances(scheduled_at, created_at)
    WHERE lock_uuid IS NULL;

CREATE INDEX IF NOT EXISTS idx_queued_instances_locked_expires_scheduled_created
    ON queued_instances(lock_expires_at, scheduled_at, created_at)
    WHERE lock_uuid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runner_instances_finished_created_instance
    ON runner_instances(created_at, instance_id)
    WHERE result IS NOT NULL OR error IS NOT NULL;
