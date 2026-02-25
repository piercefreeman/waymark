-- Remove legacy tables no longer used by runtime or webapp.

DROP TABLE IF EXISTS runner_graph_updates;
DROP TABLE IF EXISTS runner_instances_done;
