//! Poll for unpinned workloads.

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;

use crate::{HasNodeId, HasTimestamp, HasWorkloadId, PinningFor};

/// The ability to poll and pin the workloads that no node effectively
/// holds: those that are not pinned to any node, and those whose pinning
/// expired without a refresh. Polling takes such workloads over, pinning
/// them to the polling node.
///
/// Only workloads in the runnable set are polled: a parked workload
/// (see [`UnpinMode::Park`](waymark_workload_pinning_core::UnpinMode::Park))
/// has left the set and is not returned until it is made runnable again.
pub trait PollUnpinnedWorkloads: HasTimestamp + HasNodeId + HasWorkloadId {
    /// An error that can occur while polling.
    type Error: std::fmt::Debug;

    /// Return up to `max_items` workloads without blocking.
    ///
    /// Workloads are guaranteed to be freshly pinned with
    /// the provided `pinning`.
    ///
    /// `now` is the caller-clock instant `pinning.expires_at` was
    /// computed against.  Implementations keep expiry on the store's
    /// clock alone: staleness is judged against the store's own now,
    /// and the fresh expiry is stored as the store's now plus the
    /// remaining duration `expires_at - now` — a difference of two
    /// caller-clock values, so no cross-node clock agreement is needed.
    ///
    /// Returns `Ok(None)` if no workloads were available.
    fn poll_unpinned(
        &self,
        now: Self::Timestamp,
        pinning: PinningFor<Self>,
        max_items: NonZeroUsize,
    ) -> impl Future<Output = Result<Option<NEVec<Self::WorkloadId>>, Self::Error>> + Send + '_;
}
