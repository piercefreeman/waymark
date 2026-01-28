ALTER TABLE workflow_schedules
    ADD COLUMN allow_duplicate BOOLEAN NOT NULL DEFAULT false;

-- Partial index for efficient "is there a running instance for this schedule?" check.
-- Only indexes running instances with a schedule_id, so it stays small and the
-- EXISTS query becomes an index-only scan.
CREATE INDEX idx_instances_schedule_running
    ON workflow_instances(schedule_id)
    WHERE schedule_id IS NOT NULL AND status = 'running';
