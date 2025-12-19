-- Add jitter_seconds to workflow schedules for load balancing
ALTER TABLE workflow_schedules
ADD COLUMN jitter_seconds BIGINT NOT NULL DEFAULT 0;
