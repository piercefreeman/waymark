//! Core backend traits for waymark.

#[cfg(feature = "either")]
mod either;

mod data;

pub mod poll_queued_instances;

use nonempty_collections::NEVec;

use waymark_backends_core::BackendResult;
use waymark_ids::{InstanceId, LockId};

pub use self::data::*;

pub mod prelude {
    //! Prelude makes the traits known to the compiler without polluting
    //! the named items space.

    pub use crate::CoreBackend as _;
    pub use crate::poll_queued_instances::Error as _;
}

/// Abstract persistence backend for runner state.
pub trait CoreBackend {
    /// Persist updated execution graphs.
    fn save_graphs<'a>(
        &'a self,
        claim: LockClaim,
        graphs: &'a [GraphUpdate],
    ) -> impl Future<Output = BackendResult<Vec<InstanceLockStatus>>> + Send + 'a;

    /// Persist finished action attempts (success or failure).
    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> impl Future<Output = BackendResult<()>> + Send + 'a;

    /// An error that can occur while polling the queued instances.
    type PollQueuedInstancesError: poll_queued_instances::Error;

    /// Return up to size queued instances without blocking.
    fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: LockClaim,
    ) -> impl Future<Output = Result<NEVec<QueuedInstance>, Self::PollQueuedInstancesError>> + Send + '_;

    /// Refresh lock expiry for owned instances.
    fn refresh_instance_locks<'a>(
        &'a self,
        claim: LockClaim,
        instance_ids: &'a [InstanceId],
    ) -> impl Future<Output = BackendResult<Vec<InstanceLockStatus>>> + Send + 'a;

    /// Release instance locks when evicting from memory.
    fn release_instance_locks<'a>(
        &'a self,
        lock_uuid: LockId,
        instance_ids: &'a [InstanceId],
    ) -> impl Future<Output = BackendResult<()>> + Send + 'a;

    /// Persist completed workflow instances.
    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> impl Future<Output = BackendResult<()>> + Send + 'a;

    /// Insert queued instances for run-loop consumption.
    fn queue_instances<'a>(
        &'a self,
        instances: &'a [QueuedInstance],
    ) -> impl Future<Output = BackendResult<()>> + Send + 'a;
}
