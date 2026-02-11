use std::collections::{HashMap, HashSet, VecDeque};

use uuid::Uuid;

use crate::workers::ActionCompletion;

pub struct PendingPersistBatch<Step> {
    pub instance_ids: HashSet<Uuid>,
    pub steps: Vec<Step>,
}

pub enum DeferredInstanceEvent {
    Completion(ActionCompletion),
    Wake(Uuid),
}

pub struct CommitBarrier<Step> {
    next_batch_id: u64,
    blocked_instances: HashSet<Uuid>,
    pending_batches: HashMap<u64, PendingPersistBatch<Step>>,
    deferred_events: HashMap<Uuid, VecDeque<DeferredInstanceEvent>>,
}

impl<Step> Default for CommitBarrier<Step> {
    fn default() -> Self {
        Self {
            next_batch_id: 1,
            blocked_instances: HashSet::new(),
            pending_batches: HashMap::new(),
            deferred_events: HashMap::new(),
        }
    }
}

impl<Step> CommitBarrier<Step> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_batch(&mut self, instance_ids: HashSet<Uuid>, steps: Vec<Step>) -> u64 {
        let mut batch_id = self.next_batch_id;
        while self.pending_batches.contains_key(&batch_id) {
            batch_id = batch_id.wrapping_add(1);
        }
        self.next_batch_id = batch_id.wrapping_add(1);
        for instance_id in &instance_ids {
            self.blocked_instances.insert(*instance_id);
        }
        self.pending_batches.insert(
            batch_id,
            PendingPersistBatch {
                instance_ids,
                steps,
            },
        );
        batch_id
    }

    pub fn take_batch(&mut self, batch_id: u64) -> Option<PendingPersistBatch<Step>> {
        self.pending_batches.remove(&batch_id)
    }

    pub fn route_completion(&mut self, completion: ActionCompletion) -> Option<ActionCompletion> {
        if self.blocked_instances.contains(&completion.executor_id) {
            self.deferred_events
                .entry(completion.executor_id)
                .or_default()
                .push_back(DeferredInstanceEvent::Completion(completion));
            None
        } else {
            Some(completion)
        }
    }

    pub fn route_wake(&mut self, executor_id: Uuid, node_id: Uuid) -> Option<Uuid> {
        if self.blocked_instances.contains(&executor_id) {
            self.deferred_events
                .entry(executor_id)
                .or_default()
                .push_back(DeferredInstanceEvent::Wake(node_id));
            None
        } else {
            Some(node_id)
        }
    }

    pub fn unblock_instance(&mut self, instance_id: Uuid) -> VecDeque<DeferredInstanceEvent> {
        self.blocked_instances.remove(&instance_id);
        self.deferred_events
            .remove(&instance_id)
            .unwrap_or_default()
    }

    pub fn remove_instance(&mut self, instance_id: Uuid) {
        self.blocked_instances.remove(&instance_id);
        self.deferred_events.remove(&instance_id);
        for batch in self.pending_batches.values_mut() {
            batch.instance_ids.remove(&instance_id);
        }
    }

    pub fn pending_batch_count(&self) -> usize {
        self.pending_batches.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::json;
    use uuid::Uuid;

    use super::{CommitBarrier, DeferredInstanceEvent};
    use crate::workers::ActionCompletion;

    fn completion(executor_id: Uuid, execution_id: Uuid) -> ActionCompletion {
        ActionCompletion {
            executor_id,
            execution_id,
            attempt_number: 1,
            dispatch_token: Uuid::new_v4(),
            result: json!({"ok": true}),
        }
    }

    #[test]
    fn routes_directly_when_not_blocked() {
        let mut barrier: CommitBarrier<u8> = CommitBarrier::new();
        let instance_id = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let completion = completion(instance_id, execution_id);
        assert!(barrier.route_completion(completion).is_some());
        assert!(barrier.route_wake(instance_id, execution_id).is_some());
    }

    #[test]
    fn defers_while_blocked_and_flushes_on_unblock() {
        let mut barrier: CommitBarrier<u8> = CommitBarrier::new();
        let instance_id = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let batch_id = barrier.register_batch(HashSet::from([instance_id]), vec![1]);
        assert_eq!(batch_id, 1);

        let completion = completion(instance_id, execution_id);
        assert!(barrier.route_completion(completion.clone()).is_none());
        assert!(barrier.route_wake(instance_id, execution_id).is_none());

        let batch = barrier.take_batch(batch_id).expect("batch should exist");
        assert_eq!(batch.steps, vec![1]);

        let mut deferred = barrier.unblock_instance(instance_id);
        assert_eq!(deferred.len(), 2);
        match deferred.pop_front().expect("first deferred event") {
            DeferredInstanceEvent::Completion(value) => {
                assert_eq!(value.executor_id, completion.executor_id);
                assert_eq!(value.execution_id, completion.execution_id);
            }
            DeferredInstanceEvent::Wake(_) => panic!("expected completion first"),
        }
        match deferred.pop_front().expect("second deferred event") {
            DeferredInstanceEvent::Wake(node_id) => assert_eq!(node_id, execution_id),
            DeferredInstanceEvent::Completion(_) => panic!("expected wake second"),
        }
    }

    #[test]
    fn remove_instance_prunes_pending_batch_membership() {
        let mut barrier: CommitBarrier<u8> = CommitBarrier::new();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let batch_id = barrier.register_batch(HashSet::from([first, second]), vec![1, 2]);

        barrier.remove_instance(first);
        let batch = barrier.take_batch(batch_id).expect("batch should exist");
        assert!(!batch.instance_ids.contains(&first));
        assert!(batch.instance_ids.contains(&second));
    }
}
