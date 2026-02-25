//! Core backend traits for waymark.

mod data;

use uuid::Uuid;

pub use waymark_backends_core::{BackendError, BackendResult};

pub use self::data::*;

/// Abstract persistence backend for runner state.
#[async_trait::async_trait]
pub trait CoreBackend: Send + Sync {
    fn clone_box(&self) -> Box<dyn CoreBackend>;

    /// Persist updated execution graphs.
    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>>;

    /// Persist finished action attempts (success or failure).
    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()>;

    /// Return up to size queued instances without blocking.
    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch>;

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

impl Clone for Box<dyn CoreBackend> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
