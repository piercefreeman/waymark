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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuedInstance {
    pub dag: DAG,
    pub entry_node: Uuid,
    pub state: Option<RunnerState>,
    pub nodes: Option<HashMap<Uuid, ExecutionNode>>,
    pub edges: Option<HashSet<ExecutionEdge>>,
    pub action_results: Option<HashMap<Uuid, Value>>,
    #[serde(default = "default_instance_id")]
    pub instance_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceDone {
    pub executor_id: Uuid,
    pub entry_node: Uuid,
    pub result: Option<Value>,
    pub error: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphUpdate {
    pub instance_id: Uuid,
    pub nodes: HashMap<Uuid, ExecutionNode>,
    pub edges: HashSet<ExecutionEdge>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionDone {
    pub node_id: Uuid,
    pub action_name: String,
    pub attempt: i32,
    pub result: Value,
}

pub trait BaseBackend: Send + Sync {
    fn clone_box(&self) -> Box<dyn BaseBackend>;

    fn batching(&self) -> Box<dyn BaseBackend> {
        self.clone_box()
    }

    fn save_graphs<'a>(&'a self, graphs: &'a [GraphUpdate]) -> BoxFuture<'a, BackendResult<()>>;

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> BoxFuture<'a, BackendResult<()>>;

    fn get_queued_instances<'a>(
        &'a self,
        size: usize,
    ) -> BoxFuture<'a, BackendResult<Vec<QueuedInstance>>>;

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> BoxFuture<'a, BackendResult<()>>;
}

impl Clone for Box<dyn BaseBackend> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
