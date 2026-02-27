-- Workflow versions: replace dag_hash with workflow_version + ir_hash

ALTER TABLE workflow_versions
    RENAME COLUMN dag_hash TO workflow_version;

ALTER TABLE workflow_versions
    ADD COLUMN ir_hash TEXT;

UPDATE workflow_versions
SET ir_hash = workflow_version
WHERE ir_hash IS NULL;

ALTER TABLE workflow_versions
    ALTER COLUMN ir_hash SET NOT NULL;

ALTER TABLE workflow_versions
    DROP CONSTRAINT IF EXISTS workflow_versions_workflow_name_dag_hash_key;

ALTER TABLE workflow_versions
    ADD CONSTRAINT workflow_versions_workflow_name_version_key
    UNIQUE (workflow_name, workflow_version);
