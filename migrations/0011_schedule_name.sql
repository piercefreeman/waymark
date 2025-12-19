-- Migration: Add schedule_name to support multiple schedules per workflow
--
-- This allows users to create multiple schedules for the same workflow
-- with different input parameters, identified by a unique schedule_name.

-- Add schedule_name column (temporarily nullable for migration)
ALTER TABLE workflow_schedules
ADD COLUMN schedule_name TEXT;

-- Set existing schedules to use {workflow_name}-default as their schedule_name
UPDATE workflow_schedules
SET schedule_name = workflow_name || '-default';

-- Now make the column NOT NULL
ALTER TABLE workflow_schedules
ALTER COLUMN schedule_name SET NOT NULL;

-- Drop the old unique constraint on workflow_name only
ALTER TABLE workflow_schedules
DROP CONSTRAINT IF EXISTS workflow_schedules_workflow_name_key;

-- Create new unique constraint on (workflow_name, schedule_name)
-- This allows multiple schedules per workflow as long as they have different names
ALTER TABLE workflow_schedules
ADD CONSTRAINT workflow_schedules_workflow_name_schedule_name_key
UNIQUE (workflow_name, schedule_name);
