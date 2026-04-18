use std::num::NonZeroUsize;

use nonempty_collections::{IntoNonEmptyIterator, NEVec};

use waymark_ids::{InstanceId, LockId};

use crate::instance_queue_poll;

/// The ability to enqueue workflow instances for execution.
pub trait InstanceQueueProducer {
    type Error;

    /// Insert queued instances for run-loop consumption.
    fn enqueue_instances<'a>(
        &'a self,
        instances: impl Iterator<Item = InstanceForEnqueue> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// The ability to maintain the held locks on workflow instance queue work.
pub trait InstanceQueueLocksKeepalive {
    type Error;

    /// Refresh lock expiry for owned instances.
    fn refresh_instance_locks<'a>(
        &'a self,
        claim: LockClaim,
        instance_ids: impl IntoNonEmptyIterator<Item = InstanceId> + 'a,
    ) -> impl Future<Output = Result<NEVec<InstanceLockStatus>, Self::Error>> + Send + 'a;
}

/// The ability to release the held locks on workflow instance queue work.
///
/// This should not be used for instances that are done computing
pub trait InstanceQueueLocksRelease {
    type Error;

    /// Release instance locks when evicting from memory.
    fn release_instance_locks<'a>(
        &'a self,
        lock_id: LockId,
        instance_ids: impl IntoNonEmptyIterator<Item = InstanceId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// The ability to mark a workflow instance as completed, release
/// the held locks and clean up the associated execution state.
pub trait InstanceQueueCompletion {
    type Error;

    /// Persist completed workflow instances.
    fn save_instances_done<'a>(
        &'a self,
        instances: impl IntoNonEmptyIterator<Item = InstanceDone> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Execution state persistence interface.
pub trait ExecutionStatePersistence {
    type Error;

    /// Persist updated execution graphs.
    fn save_graphs<'a>(
        &'a self,
        claim: LockClaim,
        graphs: impl IntoNonEmptyIterator<Item = GraphUpdate> + 'a,
    ) -> impl Future<Output = Result<NEVec<InstanceLockStatus>, Self::Error>> + Send + 'a;
}

pub trait Full:
    InstanceQueueProducer
    + InstanceQueueConsumer
    + InstanceQueueCompletion
    + InstanceQueueLocksKeepalive
    + InstanceQueueLocksRelease
    + ExecutionStatePersistence
{
}

impl<T> Full for T where
    T: InstanceQueueProducer
        + InstanceQueueConsumer
        + InstanceQueueCompletion
        + InstanceQueueLocksKeepalive
        + InstanceQueueLocksRelease
        + ExecutionStatePersistence
{
}
