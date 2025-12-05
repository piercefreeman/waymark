-- Node readiness tracking for aggregation patterns (spread/gather, parallel blocks)
--
-- When nodes have multiple precursors that must all complete before the node can run
-- (e.g., an aggregator waiting for N spread actions), we need atomic tracking.
--
-- Flow:
-- 1. When a precursor action completes, it writes its result to node_inputs AND
--    does an atomic UPSERT here incrementing completed_count
-- 2. The UPSERT returns the new count; if count == required_count, the completing
--    action queues the dependent node as a runnable action
-- 3. The dependent node runs with its inbox already fully populated

CREATE TABLE node_readiness (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    -- The node waiting for precursors (e.g., aggregator_11)
    node_id TEXT NOT NULL,
    -- How many precursors must complete
    required_count INTEGER NOT NULL,
    -- How many have completed so far
    completed_count INTEGER NOT NULL DEFAULT 0,
    -- Timestamp
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Unique constraint for atomic upsert
    UNIQUE (instance_id, node_id)
);

-- Index for cleanup
CREATE INDEX idx_node_readiness_instance ON node_readiness(instance_id);

-- Drop the old parallel_counters table since this replaces it
DROP TABLE IF EXISTS parallel_counters;
