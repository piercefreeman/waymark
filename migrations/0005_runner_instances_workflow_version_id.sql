-- Persist workflow version on instances so webapp can show workflow names.

ALTER TABLE runner_instances
    ADD COLUMN workflow_version_id UUID;

CREATE INDEX IF NOT EXISTS idx_runner_instances_workflow_version_id
    ON runner_instances(workflow_version_id);
