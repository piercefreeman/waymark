use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
};

use nonempty_collections::NEVec;
use uuid::Uuid;
use waymark_backend_memory::MemoryBackend;
use waymark_backends_core::BackendResult;
use waymark_core_backend::{
    CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim, QueuedInstance,
};
use waymark_workflow_registry_backend::{
    WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};

#[derive(Clone)]
pub struct FaultInjectingBackend {
    inner: MemoryBackend,
    fail_get_queued_instances_with_depth_limit: Arc<AtomicBool>,
    get_queued_instances_calls: Arc<AtomicUsize>,
}

impl FaultInjectingBackend {
    pub fn with_depth_limit_poll_failures(inner: MemoryBackend) -> Self {
        Self {
            inner,
            fail_get_queued_instances_with_depth_limit: Arc::new(AtomicBool::new(true)),
            get_queued_instances_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_queued_instances_calls(&self) -> usize {
        self.get_queued_instances_calls.load(AtomicOrdering::SeqCst)
    }

    pub fn queue_len(&self) -> usize {
        self.inner
            .instance_queue()
            .as_ref()
            .map(|queue| queue.lock().expect("queue poisoned").len())
            .unwrap_or(0)
    }

    pub fn instances_done_len(&self) -> usize {
        self.inner.instances_done().len()
    }
}

#[async_trait::async_trait]
impl CoreBackend for FaultInjectingBackend {
    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.inner.save_graphs(claim, graphs).await
    }

    async fn save_actions_done(
        &self,
        actions: &[waymark_core_backend::ActionDone],
    ) -> BackendResult<()> {
        self.inner.save_actions_done(actions).await
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        self.inner.save_instances_done(instances).await
    }

    type PollQueuedInstancesError = PollQueuedInstancesError;

    async fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: LockClaim,
    ) -> Result<NEVec<QueuedInstance>, Self::PollQueuedInstancesError> {
        self.get_queued_instances_calls
            .fetch_add(1, AtomicOrdering::SeqCst);
        if self
            .fail_get_queued_instances_with_depth_limit
            .load(AtomicOrdering::SeqCst)
        {
            return Err(PollQueuedInstancesError::DepthLimitExceeded);
        }
        self.inner
            .poll_queued_instances(size, claim)
            .await
            .map_err(PollQueuedInstancesError::Memory)
    }

    async fn queue_instances(
        &self,
        instances: &[waymark_core_backend::QueuedInstance],
    ) -> BackendResult<()> {
        self.inner.queue_instances(instances).await
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.inner.refresh_instance_locks(claim, instance_ids).await
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        self.inner
            .release_instance_locks(lock_uuid, instance_ids)
            .await
    }
}

#[async_trait::async_trait]
impl WorkflowRegistryBackend for FaultInjectingBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        self.inner.upsert_workflow_version(registration).await
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        self.inner.get_workflow_versions(ids).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PollQueuedInstancesError {
    #[error("depth limit exceeded")]
    DepthLimitExceeded,

    #[error("memory backend: {0}")]
    Memory(<MemoryBackend as waymark_core_backend::CoreBackend>::PollQueuedInstancesError),
}

impl waymark_core_backend::poll_queued_instances::Error for PollQueuedInstancesError {
    fn kind(&self) -> waymark_core_backend::poll_queued_instances::ErrorKind {
        match self {
            PollQueuedInstancesError::DepthLimitExceeded => {
                waymark_core_backend::poll_queued_instances::ErrorKind::Internal
            }
            PollQueuedInstancesError::Memory(inner) => inner.kind(),
        }
    }
}
