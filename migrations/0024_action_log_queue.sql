-- Buffer table for action logs to reduce write amplification on the hot path.
-- Entries are periodically flushed into action_logs in bulk.

CREATE TABLE IF NOT EXISTS action_log_queue (
    id BIGSERIAL PRIMARY KEY,
    action_id UUID NOT NULL,
    instance_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL,
    dispatched_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    success BOOLEAN,
    result_payload BYTEA,
    error_message TEXT,
    duration_ms BIGINT,
    module_name TEXT,
    action_name TEXT,
    node_id TEXT,
    dispatch_payload BYTEA,
    pool_id UUID,
    worker_id BIGINT,
    enqueued_at TIMESTAMPTZ
);
