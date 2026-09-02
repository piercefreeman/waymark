//! Vocabulary shared by the sink and query sides.

/// The `essential_metrics_node_samples` column list, in [`NodeSample`] field order.
pub(crate) const NODE_SAMPLE_COLUMNS: &str = "node_id, sampled_at, worker_pool_size, max_in_flight_actions, in_flight_actions, \
                       queued_action_dispatches, driven_vm_runtimes, actions_completed_total, \
                       last_action_completed_at, action_dequeue_seconds_counts, action_dequeue_seconds_sum, \
                       action_handling_seconds_counts, action_handling_seconds_sum, \
                       essential_metrics_dropped_total";

/// An aggregate summing a fixed-length `bigint[]` column elementwise.
///
/// Summing histogram counts across the rows of a time bucket is the whole
/// reason these are counts rather than a quantile, but Postgres has no
/// elementwise array aggregate. The length is fixed and known here, so
/// the sum is written out one position at a time rather than reached for
/// through `unnest` — or through a function installed in the database.
pub(crate) fn elementwise_sum(column: &str, buckets: usize) -> String {
    let positions = (1..=buckets)
        .map(|position| format!("sum({column}[{position}])::bigint"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{positions}]")
}
