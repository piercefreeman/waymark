-- Rappel Core Schema
-- Single migration for new installations

-- ============================================================================
-- Workflow Versions
-- ============================================================================
-- Compiled workflow definitions with their DAG structure

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

-- ============================================================================
-- Workflow Instances
-- ============================================================================
-- Running and completed workflow executions
-- Each instance contains its entire execution state in execution_graph

CREATE TABLE workflow_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_id INT NOT NULL DEFAULT 0,
    workflow_name TEXT NOT NULL,
    workflow_version_id UUID REFERENCES workflow_versions(id) ON DELETE SET NULL,
    schedule_id UUID,  -- FK added after workflow_schedules is created
    next_action_seq INT NOT NULL DEFAULT 0,
    input_payload BYTEA,
    result_payload BYTEA,
    status TEXT NOT NULL DEFAULT 'running',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    priority INT NOT NULL DEFAULT 0,

    -- Execution graph: protobuf-encoded execution state
    -- Contains all nodes, variables, and execution history
    execution_graph BYTEA,

    -- Instance ownership for distributed execution
    owner_id TEXT,
    lease_expires_at TIMESTAMPTZ,

    -- Durable sleep scheduling
    next_wakeup_time TIMESTAMPTZ
);

-- Claim instances by priority and creation time
-- Note: We can't use NOW() in index predicates (not immutable), so the lease check
-- happens at query time. This index covers the common case of unclaimed instances.
CREATE INDEX idx_instances_claimable ON workflow_instances(priority DESC, created_at ASC)
    WHERE status = 'running';

-- Find orphaned instances (for monitoring)
CREATE INDEX idx_instances_orphaned ON workflow_instances(lease_expires_at)
    WHERE status = 'running' AND owner_id IS NOT NULL;

-- Find sleeping instances ready to wake up
CREATE INDEX idx_instances_wakeup ON workflow_instances(next_wakeup_time)
    WHERE status = 'running' AND next_wakeup_time IS NOT NULL;

-- Query by owner
CREATE INDEX idx_instances_owner ON workflow_instances(owner_id)
    WHERE owner_id IS NOT NULL;

-- Query by status
CREATE INDEX idx_instances_status ON workflow_instances(status);

-- Query by workflow name
CREATE INDEX idx_instances_workflow_name ON workflow_instances(workflow_name);

-- ============================================================================
-- Workflow Schedules
-- ============================================================================
-- Recurring workflow execution schedules

CREATE TABLE workflow_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_name TEXT NOT NULL,
    schedule_name TEXT NOT NULL,
    schedule_type TEXT NOT NULL,  -- 'cron' or 'interval'
    cron_expression TEXT,
    interval_seconds BIGINT,
    jitter_seconds BIGINT NOT NULL DEFAULT 0,
    input_payload BYTEA,
    status TEXT NOT NULL DEFAULT 'active',  -- 'active', 'paused', 'deleted'
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    last_instance_id UUID REFERENCES workflow_instances(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    priority INT NOT NULL DEFAULT 0,
    UNIQUE(workflow_name, schedule_name)
);

CREATE INDEX idx_schedules_due ON workflow_schedules(next_run_at)
    WHERE status = 'active' AND next_run_at IS NOT NULL;

-- Now add the FK from workflow_instances to workflow_schedules
ALTER TABLE workflow_instances
    ADD CONSTRAINT fk_instances_schedule
    FOREIGN KEY (schedule_id) REFERENCES workflow_schedules(id) ON DELETE SET NULL;

CREATE INDEX idx_instances_schedule ON workflow_instances(schedule_id)
    WHERE schedule_id IS NOT NULL;

-- ============================================================================
-- Worker Status
-- ============================================================================
-- Throughput and timing metrics for worker pools

CREATE TABLE worker_status (
    pool_id UUID NOT NULL,
    worker_id BIGINT NOT NULL,
    throughput_per_min DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_completed BIGINT NOT NULL DEFAULT 0,
    last_action_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    median_dequeue_ms BIGINT,
    median_handling_ms BIGINT,
    PRIMARY KEY (pool_id, worker_id)
);
