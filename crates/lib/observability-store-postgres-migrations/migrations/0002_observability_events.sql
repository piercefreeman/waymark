CREATE TABLE observability_events (
    node_id uuid NOT NULL,
    node_sequence bigint NOT NULL,
    at timestamptz NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (node_id, node_sequence)
);

CREATE INDEX observability_events_timeline_idx ON observability_events (at, node_id, node_sequence);
