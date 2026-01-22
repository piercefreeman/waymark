-- Rename dag_hash to ir_hash for clarity (it's actually the IR hash, not DAG hash)
ALTER TABLE workflow_versions RENAME COLUMN dag_hash TO ir_hash;

-- Update the unique constraint name to match
ALTER INDEX workflow_versions_workflow_name_dag_hash_key RENAME TO workflow_versions_workflow_name_ir_hash_key;
