CREATE TABLE IF NOT EXISTS worker_status (
    pool_id uuid NOT NULL,
    worker_id bigint NOT NULL,
    throughput_per_min double precision NOT NULL,
    total_completed bigint NOT NULL,
    last_action_at timestamptz NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pool_id, worker_id)
);

CREATE INDEX IF NOT EXISTS idx_worker_status_updated_at
    ON worker_status (updated_at DESC);
