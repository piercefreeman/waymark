CREATE TABLE essential_metrics_node_samples (
    node_id uuid NOT NULL,
    sampled_at timestamptz NOT NULL,
    worker_pool_size bigint NOT NULL CHECK (worker_pool_size >= 0),
    max_in_flight_actions bigint NOT NULL CHECK (max_in_flight_actions >= 0),
    in_flight_actions bigint NOT NULL CHECK (in_flight_actions >= 0),
    queued_action_dispatches bigint NOT NULL CHECK (queued_action_dispatches >= 0),
    driven_vm_runtimes bigint NOT NULL CHECK (driven_vm_runtimes >= 0),
    actions_completed_total bigint NOT NULL CHECK (actions_completed_total >= 0),
    last_action_completed_at timestamptz,
    action_dequeue_seconds_counts bigint[] NOT NULL CHECK (0 <= ALL (action_dequeue_seconds_counts)),
    action_dequeue_seconds_sum double precision NOT NULL,
    action_handling_seconds_counts bigint[] NOT NULL CHECK (0 <= ALL (action_handling_seconds_counts)),
    action_handling_seconds_sum double precision NOT NULL,
    essential_metrics_dropped_total bigint NOT NULL CHECK (essential_metrics_dropped_total >= 0),
    PRIMARY KEY (node_id, sampled_at)
);

CREATE INDEX essential_metrics_node_samples_sampled_at_idx ON essential_metrics_node_samples (sampled_at);
