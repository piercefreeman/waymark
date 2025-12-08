-- Add timeout and retry support for crash recovery.
--
-- Key additions:
-- 1. dispatched_at: When action/instance was claimed by a worker
-- 2. dispatch_token: Idempotency token to prevent stale workers from overwriting
-- 3. attempt_count: How many times this has been tried
-- 4. timeout_seconds: Per-action timeout (from decorator)

-- Actions: add dispatch tracking and retry support
ALTER TABLE actions
    ADD COLUMN dispatched_at TIMESTAMPTZ,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN timeout_seconds INTEGER NOT NULL DEFAULT 300;  -- 5 min default

-- Index for finding timed-out dispatched actions
CREATE INDEX idx_actions_timeout ON actions (status, dispatched_at)
    WHERE status = 'dispatched';

-- Instances: add dispatch tracking with idempotency token
-- Instance timeout is SHORT (30s) because instances should run-to-yield quickly.
-- They just execute until hitting actions, then immediately go back to waiting.
ALTER TABLE workflow_instances
    ADD COLUMN dispatched_at TIMESTAMPTZ,
    ADD COLUMN dispatch_token UUID,
    ADD COLUMN instance_timeout_seconds INTEGER NOT NULL DEFAULT 30;  -- 30s default (run-to-yield is fast)

-- Index for finding timed-out running instances
CREATE INDEX idx_instances_timeout ON workflow_instances (status, dispatched_at)
    WHERE status = 'running';

COMMENT ON COLUMN actions.dispatched_at IS
    'When this action was claimed by a worker. Used for timeout detection.';
COMMENT ON COLUMN actions.attempt_count IS
    'Number of times this action has been attempted. Incremented on each dispatch.';
COMMENT ON COLUMN actions.timeout_seconds IS
    'How long a worker has to complete this action before it times out.';
COMMENT ON COLUMN workflow_instances.dispatched_at IS
    'When this instance was dispatched to an instance worker. Used for timeout detection.';
COMMENT ON COLUMN workflow_instances.dispatch_token IS
    'Idempotency token. Only the worker holding this token can update the instance status. Prevents stale workers from overwriting completed state.';
