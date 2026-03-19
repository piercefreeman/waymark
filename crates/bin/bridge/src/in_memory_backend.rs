use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use uuid::Uuid;

use waymark_backends_core::{BackendError, BackendResult};
use waymark_core_backend::{
    ActionDone, CoreBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim,
    QueuedInstance,
};
use waymark_workflow_registry_backend::{
    WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};

type InstanceLockStore = HashMap<Uuid, (Option<Uuid>, Option<DateTime<Utc>>)>;
type WorkflowVersionStore = HashMap<(String, String), (Uuid, WorkflowRegistration)>;

#[derive(Clone)]
pub struct InMemoryBackend {
    queue: Arc<Mutex<VecDeque<QueuedInstance>>>,
    instances_done: Arc<Mutex<HashMap<Uuid, InstanceDone>>>,
    instance_locks: Arc<Mutex<InstanceLockStore>>,
    workflow_versions: Arc<Mutex<WorkflowVersionStore>>,
}

impl InMemoryBackend {
    pub fn new(
        queue: Arc<Mutex<VecDeque<QueuedInstance>>>,
        instances_done: Arc<Mutex<HashMap<Uuid, InstanceDone>>>,
    ) -> Self {
        Self {
            queue,
            instances_done,
            instance_locks: Arc::new(Mutex::new(HashMap::new())),
            workflow_versions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl CoreBackend for InMemoryBackend {
    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut guard = self
            .instance_locks
            .lock()
            .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
        let mut locks = Vec::with_capacity(graphs.len());
        for graph in graphs {
            if let Some((Some(lock_uuid), lock_expires_at)) = guard.get_mut(&graph.instance_id)
                && *lock_uuid == claim.lock_uuid
                && lock_expires_at.is_none_or(|expires_at| expires_at < claim.lock_expires_at)
            {
                *lock_expires_at = Some(claim.lock_expires_at);
            }
            let (lock_uuid, lock_expires_at) = guard
                .get(&graph.instance_id)
                .cloned()
                .unwrap_or((None, None));
            locks.push(InstanceLockStatus {
                instance_id: graph.instance_id,
                lock_uuid,
                lock_expires_at,
            });
        }
        Ok(locks)
    }

    async fn save_actions_done(&self, _actions: &[ActionDone]) -> BackendResult<()> {
        Ok(())
    }

    type PollQueuedInstancesError = PollQueuedInstancesError;

    async fn poll_queued_instances(
        &self,
        size: NonZeroUsize,
        claim: LockClaim,
    ) -> Result<NEVec<QueuedInstance>, Self::PollQueuedInstancesError> {
        let mut queue = self.queue.lock().expect("in-memory queue lock poisoned");

        let now = Utc::now();

        let mut take_ready_instance = || {
            queue.pop_front_if(|instance| match instance.scheduled_at {
                None => true,
                Some(scheduled_at) => scheduled_at <= now,
            })
        };

        let first_instance = take_ready_instance().ok_or(PollQueuedInstancesError::EmptyQueue)?;

        let mut instances = NEVec::with_capacity(size, first_instance);

        while instances.len() < size {
            let Some(instance) = take_ready_instance() else {
                break;
            };
            instances.push(instance);
        }

        let mut locks = self
            .instance_locks
            .lock()
            .expect("in-memory locks poisoned");

        for instance in &instances {
            locks.insert(
                instance.instance_id,
                (Some(claim.lock_uuid), Some(claim.lock_expires_at)),
            );
        }

        Ok(instances)
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let mut guard = self
            .instances_done
            .lock()
            .map_err(|_| BackendError::Message("in-memory results lock poisoned".to_string()))?;
        for instance in instances {
            guard.insert(instance.executor_id, instance.clone());
        }
        if !instances.is_empty() {
            let mut locks = self
                .instance_locks
                .lock()
                .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
            for instance in instances {
                locks.remove(&instance.executor_id);
            }
        }
        Ok(())
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        let mut guard = self
            .queue
            .lock()
            .map_err(|_| BackendError::Message("in-memory queue lock poisoned".to_string()))?;
        for instance in instances {
            guard.push_back(instance.clone());
        }
        Ok(())
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut guard = self
            .instance_locks
            .lock()
            .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
        let mut locks = Vec::new();
        for instance_id in instance_ids {
            let entry = guard
                .entry(*instance_id)
                .or_insert((Some(claim.lock_uuid), Some(claim.lock_expires_at)));
            if entry.0 == Some(claim.lock_uuid) {
                entry.1 = Some(claim.lock_expires_at);
            }
            locks.push(InstanceLockStatus {
                instance_id: *instance_id,
                lock_uuid: entry.0,
                lock_expires_at: entry.1,
            });
        }
        Ok(locks)
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        let mut guard = self
            .instance_locks
            .lock()
            .map_err(|_| BackendError::Message("in-memory locks poisoned".to_string()))?;
        for instance_id in instance_ids {
            if let Some((current_lock, _)) = guard.get(instance_id)
                && *current_lock == Some(lock_uuid)
            {
                guard.remove(instance_id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PollQueuedInstancesError {
    #[error("empty queue")]
    EmptyQueue,
}

impl waymark_core_backend::poll_queued_instances::Error for PollQueuedInstancesError {
    fn kind(&self) -> waymark_core_backend::poll_queued_instances::ErrorKind {
        match self {
            PollQueuedInstancesError::EmptyQueue => {
                waymark_core_backend::poll_queued_instances::ErrorKind::NoInstances
            }
        }
    }
}

#[async_trait::async_trait]
impl WorkflowRegistryBackend for InMemoryBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let mut guard = self.workflow_versions.lock().map_err(|_| {
            BackendError::Message("in-memory workflow versions poisoned".to_string())
        })?;
        let key = (
            registration.workflow_name.clone(),
            registration.workflow_version.clone(),
        );
        if let Some((id, existing)) = guard.get(&key) {
            if existing.ir_hash != registration.ir_hash {
                return Err(BackendError::Message(format!(
                    "workflow version already exists with different IR hash: {}@{}",
                    registration.workflow_name, registration.workflow_version
                )));
            }
            return Ok(*id);
        }
        let id = Uuid::new_v4();
        guard.insert(key, (id, registration.clone()));
        Ok(id)
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let guard = self.workflow_versions.lock().map_err(|_| {
            BackendError::Message("in-memory workflow versions poisoned".to_string())
        })?;
        let mut versions = Vec::new();
        for (id, registration) in guard.values() {
            if ids.contains(id) {
                versions.push(WorkflowVersion {
                    id: *id,
                    workflow_name: registration.workflow_name.clone(),
                    workflow_version: registration.workflow_version.clone(),
                    ir_hash: registration.ir_hash.clone(),
                    program_proto: registration.program_proto.clone(),
                    concurrent: registration.concurrent,
                });
            }
        }
        Ok(versions)
    }
}
