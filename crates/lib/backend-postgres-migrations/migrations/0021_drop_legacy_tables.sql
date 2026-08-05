-- Remove the legacy DAG-runner tables; the VM execution path owns all
-- remaining tables. The dormant scheduling wire contract (proto +
-- Python schedule API) survives; a future scheduler brings its own
-- schema.

DROP TABLE IF EXISTS runner_actions_done;
DROP TABLE IF EXISTS queued_instances;
DROP TABLE IF EXISTS runner_instances;
DROP TABLE IF EXISTS workflow_schedules;
DROP TABLE IF EXISTS workflow_versions;
