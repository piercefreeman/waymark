-- Action execution logs for tracking each run attempt with completion timestamps.
-- This is an append-only log that captures every dispatch/completion cycle,
-- including all retry attempts.

CREATE TABLE action_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    action_id UUID NOT NULL REFERENCES action_queue(id) ON DELETE CASCADE,
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    -- Which attempt this log represents (0-indexed, matches action_queue.attempt_number at dispatch time)
    attempt_number INTEGER NOT NULL,
    -- Timestamps
    dispatched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    -- Outcome (null while running)
    success BOOLEAN,
    result_payload BYTEA,
    error_message TEXT,
    -- Computed duration in milliseconds (set on completion)
    duration_ms BIGINT,

    -- Unique constraint: one log per action per attempt
    UNIQUE (action_id, attempt_number)
);

-- Index for querying logs by action
CREATE INDEX idx_action_logs_action_id ON action_logs(action_id);

-- Index for querying logs by instance (for instance history view)
CREATE INDEX idx_action_logs_instance_id ON action_logs(instance_id);

-- Index for finding recent logs (for dashboard/monitoring)
CREATE INDEX idx_action_logs_dispatched_at ON action_logs(dispatched_at DESC);
