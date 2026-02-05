//! In-memory backend that prints persistence operations.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use super::base::{
    ActionDone, BackendResult, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance,
    WorkerStatusBackend, WorkerStatusUpdate,
};

/// Backend that stores updates in memory for tests or local runs.
#[derive(Clone, Default)]
pub struct MemoryBackend {
    instance_queue: Option<Arc<Mutex<VecDeque<QueuedInstance>>>>,
    graph_updates: Arc<Mutex<Vec<GraphUpdate>>>,
    actions_done: Arc<Mutex<Vec<ActionDone>>>,
    instances_done: Arc<Mutex<Vec<InstanceDone>>>,
    worker_status_updates: Arc<Mutex<Vec<WorkerStatusUpdate>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_queue(queue: Arc<Mutex<VecDeque<QueuedInstance>>>) -> Self {
        Self {
            instance_queue: Some(queue),
            ..Self::default()
        }
    }

    pub fn instance_queue(&self) -> Option<Arc<Mutex<VecDeque<QueuedInstance>>>> {
        self.instance_queue.clone()
    }

    pub fn graph_updates(&self) -> Vec<GraphUpdate> {
        self.graph_updates
            .lock()
            .expect("graph updates poisoned")
            .clone()
    }

    pub fn actions_done(&self) -> Vec<ActionDone> {
        self.actions_done
            .lock()
            .expect("actions done poisoned")
            .clone()
    }

    pub fn instances_done(&self) -> Vec<InstanceDone> {
        self.instances_done
            .lock()
            .expect("instances done poisoned")
            .clone()
    }

    pub fn worker_status_updates(&self) -> Vec<WorkerStatusUpdate> {
        self.worker_status_updates
            .lock()
            .expect("worker status updates poisoned")
            .clone()
    }
}

impl BaseBackend for MemoryBackend {
    fn clone_box(&self) -> Box<dyn BaseBackend> {
        Box::new(self.clone())
    }

    fn save_graphs<'a>(&'a self, graphs: &'a [GraphUpdate]) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            let mut stored = self.graph_updates.lock().expect("graph updates poisoned");
            stored.extend(graphs.iter().cloned());
            Ok(())
        })
    }

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            let mut stored = self.actions_done.lock().expect("actions done poisoned");
            stored.extend(actions.iter().cloned());
            Ok(())
        })
    }

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            let mut stored = self.instances_done.lock().expect("instances done poisoned");
            stored.extend(instances.iter().cloned());
            Ok(())
        })
    }

    fn get_queued_instances<'a>(
        &'a self,
        size: usize,
    ) -> BoxFuture<'a, BackendResult<Vec<QueuedInstance>>> {
        Box::pin(async move {
            if size == 0 {
                return Ok(Vec::new());
            }
            let queue = match &self.instance_queue {
                Some(queue) => queue,
                None => return Ok(Vec::new()),
            };
            let mut guard = queue.lock().expect("instance queue poisoned");
            let mut instances = Vec::new();
            for _ in 0..size {
                if let Some(instance) = guard.pop_front() {
                    instances.push(instance);
                } else {
                    break;
                }
            }
            Ok(instances)
        })
    }
}

impl WorkerStatusBackend for MemoryBackend {
    fn upsert_worker_status<'a>(
        &'a self,
        status: &'a WorkerStatusUpdate,
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            let mut stored = self
                .worker_status_updates
                .lock()
                .expect("worker status updates poisoned");
            stored.push(status.clone());
            Ok(())
        })
    }
}
