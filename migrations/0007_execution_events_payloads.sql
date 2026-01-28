-- Execution event log and payload offload tables

ALTER TABLE workflow_instances
    ADD COLUMN snapshot_event_id BIGINT NOT NULL DEFAULT 0;

CREATE TABLE execution_payloads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    attempt_number INT NOT NULL,
    payload_kind TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_execution_payloads_instance
    ON execution_payloads(instance_id);

CREATE INDEX idx_execution_payloads_instance_node_attempt_kind
    ON execution_payloads(instance_id, node_id, attempt_number, payload_kind);

CREATE TABLE workflow_instance_events (
    id BIGSERIAL PRIMARY KEY,
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    node_id TEXT,
    worker_id TEXT,
    success BOOLEAN,
    error TEXT,
    error_type TEXT,
    duration_ms BIGINT,
    worker_duration_ms BIGINT,
    started_at_ms BIGINT,
    next_wakeup_ms BIGINT,
    inputs_payload_id UUID REFERENCES execution_payloads(id) ON DELETE SET NULL,
    result_payload_id UUID REFERENCES execution_payloads(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_instance_events_instance_id
    ON workflow_instance_events(instance_id);

CREATE INDEX idx_instance_events_instance_id_id
    ON workflow_instance_events(instance_id, id);
