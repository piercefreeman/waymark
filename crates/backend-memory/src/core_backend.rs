use chrono::Utc;
use uuid::Uuid;
use waymark_core_backend::{
    ActionDone, BackendError, BackendResult, GraphUpdate, InstanceDone, InstanceLockStatus,
    LockClaim, QueuedInstance, QueuedInstanceBatch,
};

#[async_trait::async_trait]
impl waymark_core_backend::CoreBackend for crate::MemoryBackend {
    fn clone_box(&self) -> Box<dyn waymark_core_backend::CoreBackend> {
        Box::new(self.clone())
    }

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

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        if size == 0 {
            return Ok(QueuedInstanceBatch {
                instances: Vec::new(),
            });
        }
        let queue = match &self.instance_queue {
            Some(queue) => queue,
            None => {
                return Ok(QueuedInstanceBatch {
                    instances: Vec::new(),
                });
            }
        };
        let mut guard = queue.lock().expect("instance queue poisoned");
        let now = Utc::now();
        let mut instances = Vec::new();
        while instances.len() < size {
            let Some(instance) = guard.front() else {
                break;
            };
            if let Some(scheduled_at) = instance.scheduled_at
                && scheduled_at > now
            {
                break;
            }
            let instance = guard.pop_front().expect("instance queue empty");
            instances.push(instance);
        }
        if !instances.is_empty() {
            let mut locks = self.instance_locks.lock().expect("instance locks poisoned");
            for instance in &instances {
                locks.insert(
                    instance.instance_id,
                    (Some(claim.lock_uuid), Some(claim.lock_expires_at)),
                );
            }
        }
        Ok(QueuedInstanceBatch { instances })
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
        instance_ids: &[Uuid],
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
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
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
