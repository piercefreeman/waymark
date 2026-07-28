//! Keepalive (refresh) pinnings on workloads.

use nonempty_collections::{IntoNonEmptyIterator, NEVec};

use crate::{HasNodeId, HasTimestamp, HasWorkloadId, PinningFor, PinningStatusFor};

/// The ability to maintain pinnings on workloads.
pub trait KeepalivePinnings: HasNodeId + HasWorkloadId + HasTimestamp {
    /// Error returned when refreshing pinning fails.
    type Error: std::fmt::Debug;

    /// Refresh pinning expiry for owned workloads.
    fn refresh_pinnings<'a>(
        &'a self,
        now: Self::Timestamp,
        pinning: PinningFor<Self>,
        workload_ids: impl IntoNonEmptyIterator<Item = Self::WorkloadId> + 'a,
    ) -> impl Future<Output = Result<NEVec<PinningStatusFor<Self>>, Self::Error>> + Send + 'a;
}
