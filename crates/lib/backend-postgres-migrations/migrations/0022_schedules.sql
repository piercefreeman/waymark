-- Schedules: recurring execution definitions. A schedule pins its
-- executable at registration time (the name comes from the registration;
-- the pin can never dangle) and carries its complete policy in an opaque
-- definition blob plus a baked initial runtime snapshot; spawning copies
-- the template into a fresh VM registration in one statement, fenced on
-- next_run_at.

CREATE TABLE schedules (
    schedule_name TEXT PRIMARY KEY,
    executable_id UUID NOT NULL REFERENCES vm_executables(id),
    definition BYTEA NOT NULL,
    initial_snapshot BYTEA NOT NULL,
    status TEXT NOT NULL,
    next_run_at TIMESTAMPTZ NOT NULL,
    last_instance_id UUID
);

CREATE INDEX idx_schedules_due
    ON schedules (next_run_at) WHERE status = 'active';
