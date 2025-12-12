-- Workflow schedules table
-- One active schedule per workflow_name; always runs the latest registered version
CREATE TABLE workflow_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Ties to workflow_name (NOT version), so latest version always runs
    workflow_name TEXT NOT NULL UNIQUE,

    -- Schedule type: 'cron' or 'interval'
    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('cron', 'interval')),

    -- For cron: cron expression (e.g., "0 * * * *")
    -- For interval: NULL (stored in interval_seconds instead)
    cron_expression TEXT,

    -- For interval: duration in seconds
    -- For cron: NULL
    interval_seconds BIGINT,

    -- Serialized WorkflowArguments proto (inputs for the workflow)
    input_payload BYTEA,

    -- Schedule status: 'active', 'paused', 'deleted'
    status TEXT NOT NULL DEFAULT 'active',

    -- When the schedule should next fire (computed on insert/update)
    next_run_at TIMESTAMPTZ,

    -- When the schedule last fired (NULL if never)
    last_run_at TIMESTAMPTZ,

    -- The instance_id of the most recent scheduled run (for tracking)
    last_instance_id UUID REFERENCES workflow_instances(id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints: must have either cron OR interval, not both
    CONSTRAINT schedule_cron_or_interval CHECK (
        (schedule_type = 'cron' AND cron_expression IS NOT NULL AND interval_seconds IS NULL) OR
        (schedule_type = 'interval' AND interval_seconds IS NOT NULL AND cron_expression IS NULL)
    )
);

-- Index for the scheduler loop to find due schedules efficiently
CREATE INDEX idx_workflow_schedules_next_run
    ON workflow_schedules(next_run_at)
    WHERE status = 'active' AND next_run_at IS NOT NULL;

-- Index for looking up schedule by workflow name
CREATE INDEX idx_workflow_schedules_name
    ON workflow_schedules(workflow_name);
