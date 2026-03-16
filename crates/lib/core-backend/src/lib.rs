//! Core backend traits for waymark.

mod data;

use nonempty_collections::NEVec;
use uuid::Uuid;

use waymark_backends_core::BackendResult;

pub use self::data::*;

/// Abstract persistence backend for runner state.
#[async_trait::async_trait]
pub trait CoreBackend: Send + Sync {
    /// Persist updated execution graphs.
    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>>;

    /// Persist finished action attempts (success or failure).
    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()>;

    /// Return up to size queued instances without blocking.
    #[deprecated]
    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch>;

    /// An error that can occur while polling the queued instances.
    type PollQueuedInstancesError;

    /// Return up to size queued instances without blocking.
    async fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: LockClaim,
    ) -> Result<NEVec<QueuedInstance>, PollQueuedInstancesError<Self::PollQueuedInstancesError>>;

    /// Refresh lock expiry for owned instances.
    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>>;

    /// Release instance locks when evicting from memory.
    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()>;

    /// Persist completed workflow instances.
    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()>;

    /// Insert queued instances for run-loop consumption.
    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()>;
}

#[derive(Debug, thiserror::Error)]
pub enum PollQueuedInstancesError<T> {
    /// An error indicating there were no instances.
    #[error("no instances: {0}")]
    NoInstances(#[source] T),

    /// An error indicaing some internal error condition.
    #[error("internal error: {0}")]
    Internal(#[source] T),
}

impl<T> PollQueuedInstancesError<T> {
    pub fn into_inner(self) -> T {
        match self {
            PollQueuedInstancesError::NoInstances(val)
            | PollQueuedInstancesError::Internal(val) => val,
        }
    }

    pub fn blind_map<O>(self, f: impl FnOnce(T) -> O) -> PollQueuedInstancesError<O> {
        match self {
            PollQueuedInstancesError::NoInstances(val) => {
                PollQueuedInstancesError::NoInstances(f(val))
            }
            PollQueuedInstancesError::Internal(val) => PollQueuedInstancesError::Internal(f(val)),
        }
    }
}

impl<T> AsRef<T> for PollQueuedInstancesError<T> {
    fn as_ref(&self) -> &T {
        match self {
            PollQueuedInstancesError::NoInstances(val)
            | PollQueuedInstancesError::Internal(val) => val,
        }
    }
}

impl<T> AsMut<T> for PollQueuedInstancesError<T> {
    fn as_mut(&mut self) -> &mut T {
        match self {
            PollQueuedInstancesError::NoInstances(val)
            | PollQueuedInstancesError::Internal(val) => val,
        }
    }
}
