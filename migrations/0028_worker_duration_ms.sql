-- Add worker_duration_ms to track actual Python execution time separately from
-- the full pipeline duration (which includes channel waits, batching, DB writes).
--
-- duration_ms = completed_at - dispatched_at (full pipeline time)
-- worker_duration_ms = time reported by Python worker for actual execution

ALTER TABLE action_log_queue ADD COLUMN IF NOT EXISTS worker_duration_ms BIGINT;
ALTER TABLE action_logs ADD COLUMN IF NOT EXISTS worker_duration_ms BIGINT;
