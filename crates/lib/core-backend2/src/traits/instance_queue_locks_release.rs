use nonempty_collections::IntoNonEmptyIterator;
use waymark_ids::{InstanceId, LockId};

/// The ability to release the held locks on workflow instance queue work.
///
/// This should not be used for instances that are done computing,
/// use [`InstanceQueueCompletion`] instead which covers releasing
/// the locks.
pub trait InstanceQueueLocksRelease {
    /// Error returned when releasing queue lock ownership fails.
    type Error;

    /// Release instance locks when evicting from memory.
    fn release_instance_locks<'a>(
        &'a self,
        lock_id: LockId,
        instance_ids: impl IntoNonEmptyIterator<Item = InstanceId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
