-- Persist workflow/status instance metadata for indexed search in webapp queries.

ALTER TABLE runner_instances
    ADD COLUMN IF NOT EXISTS workflow_name TEXT,
    ADD COLUMN IF NOT EXISTS current_status TEXT;

ALTER TABLE queued_instances
    ADD COLUMN IF NOT EXISTS workflow_name TEXT,
    ADD COLUMN IF NOT EXISTS current_status TEXT;

UPDATE runner_instances AS ri
SET workflow_name = wv.workflow_name
FROM workflow_versions wv
WHERE ri.workflow_name IS NULL
  AND ri.workflow_version_id = wv.id;

UPDATE runner_instances
SET current_status = CASE
    WHEN error IS NOT NULL THEN 'failed'
    WHEN result IS NOT NULL THEN 'completed'
    WHEN state IS NOT NULL THEN 'running'
    ELSE 'queued'
END
WHERE current_status IS NULL;

UPDATE queued_instances AS qi
SET workflow_name = ri.workflow_name
FROM runner_instances ri
WHERE qi.workflow_name IS NULL
  AND qi.instance_id = ri.instance_id;

UPDATE queued_instances
SET current_status = CASE
    WHEN lock_uuid IS NULL THEN 'queued'
    ELSE 'running'
END
WHERE current_status IS NULL;

ALTER TABLE runner_instances
    ADD CONSTRAINT runner_instances_current_status_check
    CHECK (
        current_status IS NULL
        OR current_status IN ('queued', 'running', 'completed', 'failed')
    );

ALTER TABLE queued_instances
    ADD CONSTRAINT queued_instances_current_status_check
    CHECK (
        current_status IS NULL
        OR current_status IN ('queued', 'running')
    );

CREATE INDEX IF NOT EXISTS idx_runner_instances_workflow_name
    ON runner_instances(workflow_name);

CREATE INDEX IF NOT EXISTS idx_runner_instances_current_status
    ON runner_instances(current_status);

CREATE INDEX IF NOT EXISTS idx_queued_instances_workflow_name
    ON queued_instances(workflow_name);

CREATE INDEX IF NOT EXISTS idx_queued_instances_current_status
    ON queued_instances(current_status);
