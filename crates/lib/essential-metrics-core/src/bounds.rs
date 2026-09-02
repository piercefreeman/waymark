//! The bucket boundaries the sampled distributions are recorded against.
//!
//! These are fixed in code rather than configured per node on purpose:
//! counts only add up across nodes and across time if every node bucketed
//! them the same way, and a boundary that could vary per deployment would
//! quietly make two nodes' samples incomparable.

/// Bucket bounds for [`NodeSample::action_dequeue_seconds`], in seconds.
///
/// Acquiring a worker slot settles in tens of microseconds when the pool
/// is not contended, so the ladder starts three decades below a
/// millisecond; the upper reaches exist only to catch contention.
///
/// [`NodeSample::action_dequeue_seconds`]: crate::NodeSample::action_dequeue_seconds
pub const ACTION_DEQUEUE_SECONDS_BOUNDS: [f64; 10] =
    [1e-5, 3e-5, 1e-4, 3e-4, 1e-3, 3e-3, 1e-2, 3e-2, 0.1, 1.0];

/// Bucket bounds for [`NodeSample::action_handling_seconds`], in seconds.
///
/// Handling is bimodal: most actions finish inside a second, while a
/// substantial minority wait on something and land in a band tens of
/// seconds wide. The lower bounds resolve the fast mode, and the 30/45/60
/// steps resolve the slow one instead of collapsing it into a single
/// bucket that hides its shape.
///
/// [`NodeSample::action_handling_seconds`]: crate::NodeSample::action_handling_seconds
pub const ACTION_HANDLING_SECONDS_BOUNDS: [f64; 12] = [
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 45.0, 60.0, 300.0,
];
