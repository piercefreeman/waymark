-- Migration: Drop legacy tables after migration to execution graph model
--
-- WARNING: Only run this migration after all instances have been migrated to
-- the new execution graph model. This is a destructive migration.
--
-- Tables being dropped:
-- - action_queue: Replaced by execution_graph.nodes + ready_queue
-- - node_inputs: Replaced by execution_graph.variables
-- - node_readiness: Replaced by execution_graph.nodes[*].waiting_for/completed_count
-- - instance_context: Replaced by execution_graph.variables
-- - loop_state: Replaced by execution_graph.nodes[*].loop_index/loop_accumulators
-- - action_log_queue: Replaced by execution_graph.nodes[*].attempts (inline history)

-- Drop indexes first
DROP INDEX IF EXISTS idx_action_queue_dispatch;
DROP INDEX IF EXISTS idx_action_queue_deadline;
DROP INDEX IF EXISTS idx_action_queue_retry;
DROP INDEX IF EXISTS idx_action_queue_instance;
DROP INDEX IF EXISTS idx_node_inputs_target;
DROP INDEX IF EXISTS idx_node_inputs_latest;
DROP INDEX IF EXISTS idx_node_inputs_spread_latest;

-- Drop tables
DROP TABLE IF EXISTS action_log_queue;
DROP TABLE IF EXISTS action_queue;
DROP TABLE IF EXISTS node_inputs;
DROP TABLE IF EXISTS node_readiness;
DROP TABLE IF EXISTS instance_context;
DROP TABLE IF EXISTS loop_state;

-- Note: action_logs table is kept for historical records
-- It can be populated from execution_graph.nodes[*].attempts if needed
