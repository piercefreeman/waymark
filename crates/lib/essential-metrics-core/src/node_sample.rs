//! The typed node sample: the closed set of essential numbers.

use crate::{
    bounds::{ACTION_DEQUEUE_SECONDS_BOUNDS, ACTION_HANDLING_SECONDS_BOUNDS},
    bucketed_histogram::BucketedHistogram,
};

/// One node's essential numbers at one instant.
///
/// Essential metrics are a closed, minimal set — the handful of numbers
/// the webapp critically needs, one field each. Counters are cumulative
/// since node start; rates are the reader's derivation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NodeSample<NodeId> {
    /// The sampling node (one identity per node boot).
    pub node_id: NodeId,

    /// When the sample was taken.
    pub sampled_at: chrono::DateTime<chrono::Utc>,

    /// Worker processes in the pool.
    pub worker_pool_size: u64,

    /// The most actions the pool can have in flight at once: the worker
    /// pool size times the per-worker concurrency.
    pub max_in_flight_actions: u64,

    /// Actions currently in flight.
    pub in_flight_actions: u64,

    /// Action dispatches accepted by the worker pool and awaiting a
    /// worker process.
    pub queued_action_dispatches: u64,

    /// Workflow VM runtimes this node currently drives: revived into
    /// memory, not yet evicted.
    pub driven_vm_runtimes: u64,

    /// Actions completed since node start.
    pub actions_completed_total: u64,

    /// When the node last completed an action; `None` before the first
    /// completion.
    pub last_action_completed_at: Option<chrono::DateTime<chrono::Utc>>,

    /// How long actions waited to be dequeued, over the interval this
    /// sample covers, bucketed by [`ACTION_DEQUEUE_SECONDS_BOUNDS`].
    pub action_dequeue_seconds: BucketedHistogram<{ ACTION_DEQUEUE_SECONDS_BOUNDS.len() + 1 }>,

    /// How long actions took to handle, over the interval this sample
    /// covers, bucketed by [`ACTION_HANDLING_SECONDS_BOUNDS`].
    pub action_handling_seconds: BucketedHistogram<{ ACTION_HANDLING_SECONDS_BOUNDS.len() + 1 }>,

    /// Samples the essential-metrics pipeline itself dropped on its own
    /// lossy path since node start.
    pub essential_metrics_dropped_total: u64,
}
