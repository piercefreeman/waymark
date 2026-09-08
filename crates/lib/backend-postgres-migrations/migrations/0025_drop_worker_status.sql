-- Remove the worker-status reporting table; essential metrics
-- (`observability.essential_metrics_node_samples`, owned by the
-- observability store) replace it.

DROP TABLE IF EXISTS worker_status;
