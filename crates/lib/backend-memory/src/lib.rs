//! In-memory backend that prints persistence operations.

#[cfg(feature = "core-backend")]
mod core_backend;

#[cfg(feature = "garbage-collector-backend")]
mod garbage_collector_backend;

#[cfg(feature = "scheduler-backend")]
mod scheduler_backend;

#[cfg(feature = "webapp-backend")]
mod webapp_backend;

#[cfg(feature = "worker-status-backend")]
mod worker_status_backend;

#[cfg(feature = "workflow-registry-backend")]
mod workflow_registry_backend;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use waymark_core_backend::{ActionDone, GraphUpdate, InstanceDone, QueuedInstance};
use waymark_scheduler_core::{ScheduleId, WorkflowSchedule};
use waymark_worker_status_backend::WorkerStatusUpdate;
use waymark_workflow_registry_backend::WorkflowRegistration;

type WorkflowVersionKey = (String, String);
type WorkflowVersionValue = (Uuid, WorkflowRegistration);
type WorkflowVersionStore = HashMap<WorkflowVersionKey, WorkflowVersionValue>;
type InstanceLockStore = HashMap<Uuid, (Option<Uuid>, Option<DateTime<Utc>>)>;

/// Backend that stores updates in memory for tests or local runs.
#[derive(Clone)]
pub struct MemoryBackend {
    instance_queue: Option<Arc<Mutex<VecDeque<QueuedInstance>>>>,
    graph_updates: Arc<Mutex<Vec<GraphUpdate>>>,
    actions_done: Arc<Mutex<Vec<ActionDone>>>,
    instances_done: Arc<Mutex<Vec<InstanceDone>>>,
    worker_status_updates: Arc<Mutex<Vec<WorkerStatusUpdate>>>,
    #[cfg_attr(not(feature = "workflow-registry-backend"), allow(dead_code))]
    workflow_versions: Arc<Mutex<WorkflowVersionStore>>,
    #[cfg_attr(not(feature = "scheduler-backend"), allow(dead_code))]
    schedules: Arc<Mutex<HashMap<ScheduleId, WorkflowSchedule>>>,
    #[cfg_attr(not(feature = "core-backend"), allow(dead_code))]
    instance_locks: Arc<Mutex<InstanceLockStore>>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self {
            instance_queue: None,
            graph_updates: Arc::new(Mutex::new(Vec::new())),
            actions_done: Arc::new(Mutex::new(Vec::new())),
            instances_done: Arc::new(Mutex::new(Vec::new())),
            worker_status_updates: Arc::new(Mutex::new(Vec::new())),
            workflow_versions: Arc::new(Mutex::new(HashMap::new())),
            schedules: Arc::new(Mutex::new(HashMap::new())),
            instance_locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
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
