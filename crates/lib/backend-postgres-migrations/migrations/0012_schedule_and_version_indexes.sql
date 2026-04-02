-- Optimize schedule admin browsing and latest workflow-version lookups.

CREATE INDEX IF NOT EXISTS idx_workflow_versions_name_created_desc_id_desc
    ON workflow_versions(workflow_name, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_workflow_schedules_active_name_schedule
    ON workflow_schedules(workflow_name, schedule_name)
    WHERE status != 'deleted';