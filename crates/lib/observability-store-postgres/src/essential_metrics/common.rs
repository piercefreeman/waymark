//! Shared by the sink and query sides.

pub mod elementwise_sum;

pub(crate) use self::elementwise_sum::elementwise_sum;

/// The `essential_metrics_node_samples` column list, in [`NodeSample`] field order.
pub(crate) const NODE_SAMPLE_COLUMNS: &str = "node_id, sampled_at, worker_pool_size, max_in_flight_actions, in_flight_actions, \
                       queued_action_dispatches, driven_vm_runtimes, actions_completed_total, \
                       last_action_completed_at, action_dequeue_seconds_counts, action_dequeue_seconds_sum, \
                       action_handling_seconds_counts, action_handling_seconds_sum, \
                       essential_metrics_dropped_total, observability_events_dropped_total";
