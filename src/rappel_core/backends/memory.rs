//! In-memory backend that prints persistence operations.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use super::base::{
    ActionDone, BackendResult, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance,
};

#[derive(Clone, Default)]
pub struct MemoryBackend {
    instance_queue: Option<Arc<Mutex<VecDeque<QueuedInstance>>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            instance_queue: None,
        }
    }

    pub fn with_queue(queue: Arc<Mutex<VecDeque<QueuedInstance>>>) -> Self {
        Self {
            instance_queue: Some(queue),
        }
    }

    pub fn instance_queue(&self) -> Option<Arc<Mutex<VecDeque<QueuedInstance>>>> {
        self.instance_queue.clone()
    }
}

impl BaseBackend for MemoryBackend {
    fn clone_box(&self) -> Box<dyn BaseBackend> {
        Box::new(self.clone())
    }

    fn save_graphs<'a>(&'a self, graphs: &'a [GraphUpdate]) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            for graph in graphs {
                println!("UPDATE {} {:?}", graph.instance_id, graph);
            }
            Ok(())
        })
    }

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            for action in actions {
                println!("INSERT {:?}", action);
            }
            Ok(())
        })
    }

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            for instance in instances {
                println!("INSERT {:?}", instance);
            }
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
