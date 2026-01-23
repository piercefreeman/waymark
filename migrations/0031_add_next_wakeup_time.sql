-- Migration: Add next_wakeup_time for durable sleep scheduling
--
-- This stores the next wakeup timestamp for instances that are fully blocked
-- on durable sleeps, allowing runners to skip claiming until they are due.

ALTER TABLE workflow_instances
    ADD COLUMN next_wakeup_time TIMESTAMPTZ;

CREATE INDEX idx_workflow_instances_next_wakeup
    ON workflow_instances (next_wakeup_time)
    WHERE status = 'running';
