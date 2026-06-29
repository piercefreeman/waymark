//! Poll for unpinned instances.

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;

use crate::{HasInstanceId, HasNodeId, HasTimestamp, PinningFor};

/// The ability to poll and pin the instances that are present in
/// the system but are not pinned to a particular node, effectively claiming
/// them.
pub trait PollUnpinnedInstances: HasTimestamp + HasNodeId + HasInstanceId {
    /// An error that can occur while polling.
    type Error: std::fmt::Debug;

    /// Return up to `max_items` instances without blocking.
    ///
    /// Instances are guaranteed to be freshly pinned with
    /// the provided `pinning`.
    ///
    /// `now` is used for expiration checks of stale pinnings.
    ///
    /// Returns `Ok(None)` if no instances were available.
    fn poll_unlocked(
        &self,
        now: Self::Timestamp,
        pinning: PinningFor<Self>,
        max_items: NonZeroUsize,
    ) -> impl Future<Output = Result<Option<NEVec<Self::InstanceId>>, Self::Error>> + Send + '_;
}
