//! Keepalive (refresh) pinnings on instances.

use nonempty_collections::{IntoNonEmptyIterator, NEVec};

use crate::{HasInstanceId, HasNodeId, HasTimestamp, PinningFor, PinningStatusFor};

/// The ability to maintain pins on instances.
pub trait KeepaliveInstancePinnings: HasNodeId + HasInstanceId + HasTimestamp {
    /// Error returned when refreshing pinning fails.
    type Error: std::fmt::Debug;

    /// Refresh pinning expiry for owned instances.
    fn refresh_pinnings<'a>(
        &'a self,
        now: Self::Timestamp,
        pinning: PinningFor<Self>,
        instance_ids: impl IntoNonEmptyIterator<Item = Self::InstanceId> + 'a,
    ) -> impl Future<Output = Result<NEVec<PinningStatusFor<Self>>, Self::Error>> + Send + 'a;
}
