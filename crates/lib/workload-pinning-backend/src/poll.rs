//! Poll for unpinned instances.

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;

use crate::{HasInstanceId, HasNodeId, HasTimestamp, PinningFor};

/// The ability to poll and pin the instances that are present in
/// the system but are not pinned to a particular node, effectively claiming
/// them.
pub trait PollUnpinnedInstances: HasTimestamp + HasNodeId + HasInstanceId {
    /// An error that can occur while polling.
    type Error: Error;

    /// Return up to `max_items` instances without blocking.
    ///
    /// Instances are guaranteed to be freshly pinned with
    /// the provided `pinning`.
    ///
    /// `now` is used for expiration checks of stale pinnings.
    fn poll_unlocked(
        &self,
        now: Self::Timestamp,
        pinning: PinningFor<Self>,
        max_items: NonZeroUsize,
    ) -> impl Future<Output = Result<NEVec<Self::InstanceId>, Self::Error>> + Send + '_;
}

/// Classification interface for [`PollUnpinnedInstances`] backend errors.
pub trait Error {
    /// Get the classification for the error kind.
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for [`PollUnpinnedInstances`] backend failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// No instances were available.
    NoInstances,

    /// An internal backend failure occurred.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        match *self {}
    }
}
