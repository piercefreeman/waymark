//! Poll for unpinned workloads.

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;

use crate::{HasNodeId, HasTimestamp, HasWorkloadId, PinningFor};

/// The ability to poll and pin the workloads that no node effectively
/// holds: those that are not pinned to any node, and those whose pinning
/// expired without a refresh. Polling takes such workloads over, pinning
/// them to the polling node.
pub trait PollUnpinnedWorkloads: HasTimestamp + HasNodeId + HasWorkloadId {
    /// An error that can occur while polling.
    type Error: std::fmt::Debug;

    /// Return up to `max_items` workloads without blocking.
    ///
    /// Workloads are guaranteed to be freshly pinned with
    /// the provided `pinning`.
    ///
    /// `now` is used for expiration checks of stale pinnings.
    ///
    /// Returns `Ok(None)` if no workloads were available.
    fn poll_unpinned(
        &self,
        now: Self::Timestamp,
        pinning: PinningFor<Self>,
        max_items: NonZeroUsize,
    ) -> impl Future<Output = Result<Option<NEVec<Self::WorkloadId>>, Self::Error>> + Send + '_;
}
