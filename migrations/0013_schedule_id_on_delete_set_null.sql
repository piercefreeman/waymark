-- Migration: Allow deleting schedules without deleting instances
ALTER TABLE workflow_instances
DROP CONSTRAINT IF EXISTS workflow_instances_schedule_id_fkey;

ALTER TABLE workflow_instances
ADD CONSTRAINT workflow_instances_schedule_id_fkey
    FOREIGN KEY (schedule_id)
    REFERENCES workflow_schedules(id)
    ON DELETE SET NULL;
