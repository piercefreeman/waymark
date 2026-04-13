use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
};

use nonempty_collections::NEVec;
use waymark_backends_core::BackendResult;
use waymark_core_backend::{
    CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim, QueuedInstance,
};
use waymark_ids::{InstanceId, LockId, WorkflowVersionId};
use waymark_workflow_registry_backend::{
    WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};

#[derive(Clone)]
pub struct FaultInjectingBackend<Inner> {
    inner: Inner,
    fail_get_queued_instances_with_depth_limit: Arc<AtomicBool>,
    get_queued_instances_calls: Arc<AtomicUsize>,
}

impl<Inner> FaultInjectingBackend<Inner> {
    pub fn with_depth_limit_poll_failures(inner: Inner) -> Self {
        Self {
            inner,
            fail_get_queued_instances_with_depth_limit: Arc::new(AtomicBool::new(true)),
            get_queued_instances_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_queued_instances_calls(&self) -> usize {
        self.get_queued_instances_calls.load(AtomicOrdering::SeqCst)
    }
}

impl<Inner> AsRef<Inner> for FaultInjectingBackend<Inner> {
    fn as_ref(&self) -> &Inner {
        &self.inner
    }
}

impl<Inner> CoreBackend for FaultInjectingBackend<Inner>
where
    Inner: CoreBackend + Sync,
{
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

    type PollQueuedInstancesError = PollQueuedInstancesError<Inner::PollQueuedInstancesError>;

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
            .map_err(PollQueuedInstancesError::Inner)
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
        instance_ids: &[InstanceId],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.inner.refresh_instance_locks(claim, instance_ids).await
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: LockId,
        instance_ids: &[InstanceId],
    ) -> BackendResult<()> {
        self.inner
            .release_instance_locks(lock_uuid, instance_ids)
            .await
    }
}

impl<Inner> WorkflowRegistryBackend for FaultInjectingBackend<Inner>
where
    Inner: WorkflowRegistryBackend + Sync,
{
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<WorkflowVersionId> {
        self.inner.upsert_workflow_version(registration).await
    }

    async fn get_workflow_versions(
        &self,
        ids: &[WorkflowVersionId],
    ) -> BackendResult<Vec<WorkflowVersion>> {
        self.inner.get_workflow_versions(ids).await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PollQueuedInstancesError<InnerError> {
    #[error("depth limit exceeded")]
    DepthLimitExceeded,

    #[error("inner backend: {0}")]
    Inner(InnerError),
}

impl<InnerError> waymark_core_backend::poll_queued_instances::Error
    for PollQueuedInstancesError<InnerError>
where
    InnerError: waymark_core_backend::poll_queued_instances::Error,
{
    fn kind(&self) -> waymark_core_backend::poll_queued_instances::ErrorKind {
        match self {
            PollQueuedInstancesError::DepthLimitExceeded => {
                waymark_core_backend::poll_queued_instances::ErrorKind::Internal
            }
            PollQueuedInstancesError::Inner(inner) => inner.kind(),
        }
    }
}
