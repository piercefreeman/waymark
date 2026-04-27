use nonempty_collections::{IntoNonEmptyIterator, NEVec};
use waymark_ids::InstanceId;

use crate::{InstanceLockStatus, LockClaim};

/// The ability to maintain the held locks on workflow instance queue work.
pub trait InstanceQueueLocksKeepalive {
    /// Error returned when refreshing queue lock ownership fails.
    type Error;

    /// Refresh lock expiry for owned instances.
    fn refresh_instance_locks<'a>(
        &'a self,
        claim: LockClaim,
        instance_ids: impl IntoNonEmptyIterator<Item = InstanceId> + 'a,
    ) -> impl Future<Output = Result<NEVec<InstanceLockStatus>, Self::Error>> + Send + 'a;
}
