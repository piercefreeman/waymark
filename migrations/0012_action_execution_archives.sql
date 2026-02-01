-- Action execution archives store per-execution metadata without payloads.
-- Payloads live in node_payloads to avoid heavy graph updates.

CREATE TABLE action_execution_archives (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL,
    node_id TEXT NOT NULL,
    action_id TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    status INT NOT NULL,
    attempt_number INT NOT NULL,
    max_retries INT NOT NULL,
    worker_id TEXT,
    started_at_ms BIGINT,
    completed_at_ms BIGINT,
    duration_ms BIGINT,
    parent_execution_id TEXT,
    spread_index INT,
    loop_index INT,
    waiting_for JSONB NOT NULL DEFAULT '[]'::jsonb,
    completed_count INT NOT NULL DEFAULT 0,
    node_kind INT NOT NULL,
    error TEXT,
    error_type TEXT,
    timeout_seconds INT NOT NULL,
    timeout_retry_limit INT NOT NULL,
    backoff_kind INT NOT NULL,
    backoff_base_delay_ms INT NOT NULL,
    backoff_multiplier DOUBLE PRECISION NOT NULL,
    sleep_wakeup_time_ms BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(id) ON DELETE CASCADE,
    UNIQUE (instance_id, execution_id)
);

CREATE INDEX idx_action_execution_archives_instance
    ON action_execution_archives(instance_id);

CREATE INDEX idx_action_execution_archives_execution
    ON action_execution_archives(execution_id);
