//! Core backend traits for waymark.

mod data;
pub mod poll_queued_instances;

use nonempty_collections::NEVec;
use uuid::Uuid;

use waymark_backends_core::BackendResult;

pub use self::data::*;

pub mod prelude {
    //! Prelude makes the traits known to the compiler without polluting
    //! the named items space.

    pub use crate::CoreBackend as _;
    pub use crate::poll_queued_instances::Error as _;
}

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

    /// An error that can occur while polling the queued instances.
    type PollQueuedInstancesError: poll_queued_instances::Error;

    /// Return up to size queued instances without blocking.
    async fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: LockClaim,
    ) -> Result<NEVec<QueuedInstance>, Self::PollQueuedInstancesError>;

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
