-- Archive table for completed/failed workflow instances
-- Stores the full execution graph (all inputs/outputs) for historical queries
-- while keeping workflow_instances lean as a worker queue

CREATE TABLE completed_instances (
    id UUID PRIMARY KEY,
    partition_id INTEGER NOT NULL,
    workflow_name TEXT NOT NULL,
    workflow_version_id UUID,
    schedule_id UUID,
    input_payload BYTEA,
    result_payload BYTEA,
    status TEXT NOT NULL,  -- 'completed' or 'failed'
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority INTEGER NOT NULL DEFAULT 0,
    -- Full execution graph with all inputs/outputs
    execution_graph BYTEA,
    FOREIGN KEY (workflow_version_id) REFERENCES workflow_versions(id) ON DELETE SET NULL
);

-- For listing instances by workflow
CREATE INDEX idx_completed_instances_workflow ON completed_instances(workflow_name, completed_at DESC);

-- For listing recent completions
CREATE INDEX idx_completed_instances_completed_at ON completed_instances(completed_at DESC);

-- For schedule history
CREATE INDEX idx_completed_instances_schedule ON completed_instances(schedule_id, completed_at DESC);
