//! Wire vocabulary shared by the nodes operations.

/// A distribution over bucket boundaries, as served on the wire.
///
/// The bounds travel with the counts so a reader needs nothing else to
/// make sense of them, and `p50` is interpolated here rather than by the
/// reader — after any aggregation, which is the point of shipping counts
/// rather than a median.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Histogram {
    /// Upper bound of each bucket, in seconds, ascending.
    pub bounds: Vec<f64>,

    /// Observations at or below each corresponding bound, then the
    /// total — one more entry than there are bounds.
    pub counts: Vec<u64>,

    /// The observed values added together, for deriving a mean.
    pub sum: f64,

    /// The interpolated median; absent when nothing was observed, or
    /// when the median falls above the last bound.
    pub p50: Option<f64>,
}

fn histogram<const N: usize>(
    histogram: waymark_essential_metrics_core::BucketedHistogram<N>,
    bounds: &[f64],
) -> Histogram {
    Histogram {
        bounds: bounds.to_vec(),
        counts: histogram.counts.to_vec(),
        sum: histogram.sum,
        p50: histogram.quantile(bounds, 0.5),
    }
}

/// One node's essential numbers at one instant, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct NodeSample {
    /// The sampling node's id (a UUID; one identity per node boot).
    pub node_id: String,

    /// When the sample was taken.
    pub sampled_at: chrono::DateTime<chrono::Utc>,

    /// Worker processes in the pool.
    pub worker_pool_size: u64,

    /// The most actions the pool can have in flight at once.
    pub max_in_flight_actions: u64,

    /// Actions currently in flight.
    pub in_flight_actions: u64,

    /// Action dispatches accepted by the worker pool and awaiting a
    /// worker process.
    pub queued_action_dispatches: u64,

    /// Workflow VM runtimes the node currently drives.
    pub driven_vm_runtimes: u64,

    /// Actions completed since node start.
    pub actions_completed_total: u64,

    /// When the node last completed an action; absent before the first
    /// completion.
    pub last_action_completed_at: Option<chrono::DateTime<chrono::Utc>>,

    /// How long actions waited to be dequeued, over the interval this
    /// sample covers.
    pub action_dequeue_seconds: Histogram,

    /// How long actions took to handle, over the interval this sample
    /// covers.
    pub action_handling_seconds: Histogram,

    /// Samples the essential-metrics pipeline itself dropped on its own
    /// lossy path since node start.
    pub essential_metrics_dropped_total: u64,
}

pub(crate) fn node_sample(
    sample: waymark_essential_metrics_core::NodeSample<waymark_ids::NodeId>,
) -> NodeSample {
    NodeSample {
        node_id: sample.node_id.to_string(),
        sampled_at: sample.sampled_at,
        worker_pool_size: sample.worker_pool_size,
        max_in_flight_actions: sample.max_in_flight_actions,
        in_flight_actions: sample.in_flight_actions,
        queued_action_dispatches: sample.queued_action_dispatches,
        driven_vm_runtimes: sample.driven_vm_runtimes,
        actions_completed_total: sample.actions_completed_total,
        last_action_completed_at: sample.last_action_completed_at,
        action_dequeue_seconds: histogram(
            sample.action_dequeue_seconds,
            &waymark_essential_metrics_core::ACTION_DEQUEUE_SECONDS_BOUNDS,
        ),
        action_handling_seconds: histogram(
            sample.action_handling_seconds,
            &waymark_essential_metrics_core::ACTION_HANDLING_SECONDS_BOUNDS,
        ),
        essential_metrics_dropped_total: sample.essential_metrics_dropped_total,
    }
}
