CREATE TABLE essential_metrics_node_samples (
    node_id uuid NOT NULL,
    sampled_at timestamptz NOT NULL,
    worker_pool_size bigint NOT NULL,
    max_in_flight_actions bigint NOT NULL,
    in_flight_actions bigint NOT NULL,
    queued_action_dispatches bigint NOT NULL,
    driven_vm_runtimes bigint NOT NULL,
    actions_completed_total bigint NOT NULL,
    last_action_completed_at timestamptz,
    action_dequeue_seconds_counts bigint[] NOT NULL,
    action_dequeue_seconds_sum double precision NOT NULL,
    action_handling_seconds_counts bigint[] NOT NULL,
    action_handling_seconds_sum double precision NOT NULL,
    essential_metrics_dropped_total bigint NOT NULL,
    PRIMARY KEY (node_id, sampled_at)
);

CREATE INDEX essential_metrics_node_samples_sampled_at_idx ON essential_metrics_node_samples (sampled_at);
