//! The poll → maintenance dispatch message.
//!
//! See [`PinnedBatch`].

use nonempty_collections::NEVec;
use tokio_util::sync::CancellationToken;

/// A batch of newly pinned workloads dispatched from the poll loop to
/// the maintenance loop.
pub(crate) struct PinnedBatch<WorkloadId> {
    /// The local monotonic instant captured immediately before the
    /// pinning call was issued — the conservative base for the fence
    /// deadlines of these workloads.
    pub pinned_at: tokio::time::Instant,

    /// The newly pinned workload IDs, each with the fence token the
    /// maintenance loop cancels when the pinning can no longer be
    /// proven held.  The matching [`PinnedHandle`](crate::PinnedHandle)
    /// holds a clone of the token.
    pub pinned: NEVec<(WorkloadId, CancellationToken)>,

    /// Replies with the updated active-workload count.
    pub reply: tokio::sync::oneshot::Sender<usize>,
}
