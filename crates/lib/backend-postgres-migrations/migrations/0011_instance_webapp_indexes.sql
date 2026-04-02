-- Optimize instance history browsing and latest action-result lookups.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_runner_instances_created_desc_instance_desc
    ON runner_instances(created_at DESC, instance_id DESC);

CREATE INDEX IF NOT EXISTS idx_runner_instances_workflow_name_trgm
    ON runner_instances USING gin(workflow_name gin_trgm_ops)
    WHERE workflow_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_runner_actions_done_latest_by_execution
    ON runner_actions_done(execution_id, attempt DESC, id DESC);
