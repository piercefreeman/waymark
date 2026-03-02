-- Waymark core schema (baseline)

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- Workflow definitions
-- ---------------------------------------------------------------------------

CREATE TABLE workflow_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name TEXT NOT NULL,
    dag_hash TEXT NOT NULL,
    program_proto BYTEA NOT NULL,
    concurrent BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(workflow_name, dag_hash)
);

CREATE INDEX idx_workflow_versions_name ON workflow_versions(workflow_name);

-- ---------------------------------------------------------------------------
-- Runner persistence tables
-- ---------------------------------------------------------------------------

CREATE TABLE runner_graph_updates (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state BYTEA NOT NULL
);

CREATE TABLE runner_actions_done (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    node_id UUID NOT NULL,
    action_name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    result BYTEA
);

CREATE TABLE runner_instances (
    instance_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    entry_node UUID NOT NULL,
    state BYTEA,
    result BYTEA,
    error BYTEA
);

CREATE TABLE runner_instances_done (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executor_id UUID NOT NULL,
    entry_node UUID NOT NULL,
    result BYTEA,
    error BYTEA
);

CREATE TABLE queued_instances (
    instance_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload BYTEA NOT NULL
);

-- ---------------------------------------------------------------------------
-- Scheduler
-- ---------------------------------------------------------------------------

CREATE TABLE workflow_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name TEXT NOT NULL,
    schedule_name TEXT NOT NULL,
    schedule_type TEXT NOT NULL,
    cron_expression TEXT,
    interval_seconds BIGINT,
    jitter_seconds BIGINT NOT NULL DEFAULT 0,
    input_payload BYTEA,
    status TEXT NOT NULL DEFAULT 'active',
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    last_instance_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority INT NOT NULL DEFAULT 0,
    allow_duplicate BOOLEAN NOT NULL DEFAULT false,
    UNIQUE(workflow_name, schedule_name)
);

CREATE INDEX idx_schedules_due ON workflow_schedules(next_run_at)
    WHERE status = 'active' AND next_run_at IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Worker status metrics
-- ---------------------------------------------------------------------------

CREATE TABLE worker_status (
    pool_id UUID NOT NULL,
    worker_id BIGINT NOT NULL,
    throughput_per_min DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_completed BIGINT NOT NULL DEFAULT 0,
    last_action_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    median_dequeue_ms BIGINT,
    median_handling_ms BIGINT,
    dispatch_queue_size BIGINT,
    total_in_flight BIGINT,
    active_workers INT NOT NULL DEFAULT 0,
    actions_per_sec DOUBLE PRECISION NOT NULL DEFAULT 0,
    median_instance_duration_secs DOUBLE PRECISION,
    active_instance_count INT NOT NULL DEFAULT 0,
    total_instances_completed BIGINT NOT NULL DEFAULT 0,
    instances_per_sec DOUBLE PRECISION NOT NULL DEFAULT 0,
    instances_per_min DOUBLE PRECISION NOT NULL DEFAULT 0,
    time_series BYTEA,
    PRIMARY KEY (pool_id, worker_id)
);
