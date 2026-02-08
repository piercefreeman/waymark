-- Persist per-attempt lifecycle metadata for action history and timeline rendering.

ALTER TABLE runner_actions_done
    ADD COLUMN status TEXT,
    ADD COLUMN started_at TIMESTAMPTZ,
    ADD COLUMN completed_at TIMESTAMPTZ,
    ADD COLUMN duration_ms BIGINT;

ALTER TABLE runner_actions_done
    ADD CONSTRAINT runner_actions_done_status_check
    CHECK (status IS NULL OR status IN ('completed', 'failed', 'timed_out'));

CREATE INDEX idx_runner_actions_done_execution_attempt
    ON runner_actions_done (execution_id, attempt);
