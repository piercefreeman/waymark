-- Add scheduling and locking for queued instances.

ALTER TABLE queued_instances
    ADD COLUMN scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN lock_uuid UUID,
    ADD COLUMN lock_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_queued_instances_scheduled_at
    ON queued_instances(scheduled_at);

CREATE INDEX IF NOT EXISTS idx_queued_instances_lock_expires_at
    ON queued_instances(lock_expires_at);
