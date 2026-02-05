//! Backend interfaces for persisting runner state and action results.

use std::collections::{HashMap, HashSet};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::rappel_core::dag::DAG;
use crate::rappel_core::runner::state::{ExecutionEdge, ExecutionNode, RunnerState};

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

pub type BackendResult<T> = Result<T, BackendError>;

fn default_instance_id() -> Uuid {
    Uuid::new_v4()
}

// The models that we use for our backends are similar to the ones that we
// have specified in our database/Postgres backend, but not 1:1. It's better for
// us to internally convert within the given backend

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Queued instance payload for the run loop.
pub struct QueuedInstance {
    pub dag: DAG,
    pub entry_node: Uuid,
    pub state: Option<RunnerState>,

    // TODO: Isn't nodes & edges here just repetitive from the DAG+RunnerState?
    pub nodes: Option<HashMap<Uuid, ExecutionNode>>,
    pub edges: Option<HashSet<ExecutionEdge>>,

    pub action_results: Option<HashMap<Uuid, Value>>,
    #[serde(default = "default_instance_id")]
    pub instance_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Completed instance payload with result or exception.
pub struct InstanceDone {
    pub executor_id: Uuid,
    pub entry_node: Uuid,
    pub result: Option<Value>,
    pub error: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Batch payload representing an updated execution graph snapshot.
///
/// This intentionally stores only runtime nodes and edges (no DAG template or
/// derived caches) so persistence stays lightweight.
pub struct GraphUpdate {
    pub instance_id: Uuid,
    // TODO: Should we just be passed the RunnerState here so it's already
    // nice and typehinted for us?
    pub nodes: HashMap<Uuid, ExecutionNode>,
    pub edges: HashSet<ExecutionEdge>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Batch payload representing a completed action execution.
pub struct ActionDone {
    // TODO: I don't think we need the action_name... Additionally, done
    // actions should NOT be parameterized based on the node_id since the node
    // is only the ground truth DAG representation. Instead we should have execution_id.
    pub node_id: Uuid,
    pub action_name: String,
    pub attempt: i32,
    pub result: Value,
}

/// Worker status update for persistence.
#[derive(Clone, Debug)]
pub struct WorkerStatusUpdate {
    pub pool_id: Uuid,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<chrono::DateTime<chrono::Utc>>,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
    pub dispatch_queue_size: i64,
    pub total_in_flight: i64,
    pub active_workers: i32,
    pub actions_per_sec: f64,
    pub median_instance_duration_secs: Option<f64>,
    pub active_instance_count: i32,
    pub total_instances_completed: i64,
    pub instances_per_sec: f64,
    pub instances_per_min: f64,
    pub time_series: Option<Vec<u8>>,
}

/// Backend capability for recording worker status metrics.
pub trait WorkerStatusBackend: Send + Sync {
    fn upsert_worker_status<'a>(
        &'a self,
        status: &'a WorkerStatusUpdate,
    ) -> BoxFuture<'a, BackendResult<()>>;
}

// TODO: Refactor backends to be in src/backends... The active backend should be passed
// into the rappel_core entrypoint & the scheduled entrypoint & the workers entrypoint

/// Abstract persistence backend for runner state.
pub trait BaseBackend: Send + Sync {
    fn clone_box(&self) -> Box<dyn BaseBackend>;

    // TODO: Switch all of these calls to just use async as their public definition... this avoids
    // the need to capture these box futures and leads to a much cleaner signature
    /// Persist updated execution graphs.
    fn save_graphs<'a>(&'a self, graphs: &'a [GraphUpdate]) -> BoxFuture<'a, BackendResult<()>>;

    /// Persist completed action executions.
    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> BoxFuture<'a, BackendResult<()>>;

    /// Return up to size queued instances without blocking.
    fn get_queued_instances<'a>(
        &'a self,
        size: usize,
    ) -> BoxFuture<'a, BackendResult<Vec<QueuedInstance>>>;

    /// Persist completed workflow instances.
    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> BoxFuture<'a, BackendResult<()>>;

    // TODO: Add endpoint for registering workflow DAGs
    // TODO: Add endpoint for registering new instances
    // TODO: Add endpoint for saving worker status updates
    // TODO: Add endpoint for adding a new schedule
    // TODO: Add endpoint for getting ready schedules
}

impl Clone for Box<dyn BaseBackend> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
