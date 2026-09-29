-- The observability events a node's observability-events pipeline dropped,
-- carried in its samples like the essential-metrics pipeline's own drops.
-- Rows from before the column read as 0.
ALTER TABLE essential_metrics_node_samples
    ADD COLUMN observability_events_dropped_total bigint NOT NULL DEFAULT 0
        CHECK (observability_events_dropped_total >= 0);
