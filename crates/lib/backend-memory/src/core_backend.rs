use chrono::Utc;
use nonempty_collections::NEVec;
use waymark_backends_core::{BackendError, BackendResult};
use waymark_core_backend::{
    ActionDone, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim, QueuedInstance,
};
use waymark_ids::{InstanceId, LockId};

impl waymark_core_backend::CoreBackend for crate::MemoryBackend {
    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut stored = self.graph_updates.lock().expect("graph updates poisoned");
        stored.extend(graphs.iter().cloned());
        let mut guard = self.instance_locks.lock().expect("instance locks poisoned");
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

    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()> {
        let mut stored = self.actions_done.lock().expect("actions done poisoned");
        stored.extend(actions.iter().cloned());
        Ok(())
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        let mut stored = self.instances_done.lock().expect("instances done poisoned");
        stored.extend(instances.iter().cloned());
        if !instances.is_empty() {
            let mut locks = self.instance_locks.lock().expect("instance locks poisoned");
            for instance in instances {
                locks.remove(&instance.executor_id);
            }
        }
        Ok(())
    }

    type PollQueuedInstancesError = PollQueuedInstancesError;

    async fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: LockClaim,
    ) -> Result<NEVec<QueuedInstance>, Self::PollQueuedInstancesError> {
        let queue = self
            .instance_queue
            .as_ref()
            .ok_or(PollQueuedInstancesError::NoQueue)?;

        let mut queue = queue.lock().expect("instance queue poisoned");

        let now = Utc::now();

        let mut take_ready_instance = || {
            queue.pop_front_if(|instance| match instance.scheduled_at {
                None => true,
                Some(scheduled_at) => scheduled_at <= now,
            })
        };

        let first_instance = take_ready_instance().ok_or(PollQueuedInstancesError::QueueEmpty)?;

        let mut instances = NEVec::with_capacity(size, first_instance);
        while instances.len() < size {
            let Some(instance) = take_ready_instance() else {
                break;
            };
            instances.push(instance);
        }

        let mut locks = self.instance_locks.lock().expect("instance locks poisoned");
        for instance in &instances {
            locks.insert(
                instance.instance_id,
                (Some(claim.lock_uuid), Some(claim.lock_expires_at)),
            );
        }

        Ok(instances)
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        if instances.is_empty() {
            return Ok(());
        }
        let queue = self.instance_queue.as_ref().ok_or_else(|| {
            BackendError::Message("memory backend missing instance queue".to_string())
        })?;
        let mut guard = queue.lock().expect("instance queue poisoned");
        for instance in instances {
            guard.push_back(instance.clone());
        }
        Ok(())
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[InstanceId],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        let mut guard = self.instance_locks.lock().expect("instance locks poisoned");
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
        lock_uuid: LockId,
        instance_ids: &[InstanceId],
    ) -> BackendResult<()> {
        let mut guard = self.instance_locks.lock().expect("instance locks poisoned");
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
    #[error("backend is instantiated with no queue")]
    NoQueue,

    #[error("queue is empty")]
    QueueEmpty,
}

impl waymark_core_backend::poll_queued_instances::Error for PollQueuedInstancesError {
    fn kind(&self) -> waymark_core_backend::poll_queued_instances::ErrorKind {
        // For the compatibility reasons, any error from the memory backend is
        // treated as no instances; this may be revisited in the future.
        match self {
            PollQueuedInstancesError::NoQueue | PollQueuedInstancesError::QueueEmpty => {
                waymark_core_backend::poll_queued_instances::ErrorKind::NoInstances
            }
        }
    }
}
