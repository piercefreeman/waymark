-- Push-based scheduling tables
-- Instead of re-evaluating all completed actions on each scheduling call,
-- we push results to downstream nodes and track ready state incrementally.

-- Tracks which nodes are ready to execute based on dependency satisfaction
CREATE TABLE node_ready_state (
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    deps_required SMALLINT NOT NULL,
    deps_satisfied SMALLINT NOT NULL DEFAULT 0,
    is_queued BOOLEAN NOT NULL DEFAULT FALSE,
    is_completed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (instance_id, node_id)
);

-- Index for finding ready-to-dispatch nodes
CREATE INDEX idx_node_ready_state_ready
    ON node_ready_state (instance_id, is_queued, is_completed)
    WHERE is_queued = FALSE AND is_completed = FALSE;

-- Stores context data pushed from upstream nodes to downstream nodes
-- This is the pre-built context that will be used when dispatching
CREATE TABLE node_pending_context (
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL,  -- which upstream node provided this
    variable TEXT NOT NULL,
    payload BYTEA,
    PRIMARY KEY (instance_id, node_id, source_node_id, variable)
);

-- Index for fetching all context for a node
CREATE INDEX idx_node_pending_context_node
    ON node_pending_context (instance_id, node_id);

-- Materialized loop state - instead of reconstructing from action history
CREATE TABLE loop_iteration_state (
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    current_index INT NOT NULL DEFAULT 0,
    accumulator BYTEA,  -- serialized JSON array of results
    completed_phases BYTEA,  -- for multi-action loops: serialized set of phase IDs
    phase_results BYTEA,  -- for multi-action loops: serialized map of phase -> result
    preamble_results BYTEA,  -- for multi-action loops: serialized map of var -> value
    PRIMARY KEY (instance_id, node_id)
);

-- Evaluation context snapshot - stores the eval_ctx built up so far
-- This avoids re-extracting variables from all completed actions
CREATE TABLE instance_eval_context (
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    context_json JSONB NOT NULL DEFAULT '{}',
    exceptions_json JSONB NOT NULL DEFAULT '{}',
    PRIMARY KEY (instance_id)
);
