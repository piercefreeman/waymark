//! Worker pool interface for executing actions.

use std::collections::HashMap;

use futures::future::BoxFuture;
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ActionRequest {
    pub executor_id: Uuid,
    pub node_id: Uuid,
    pub action_name: String,
    pub kwargs: HashMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct ActionCompletion {
    pub executor_id: Uuid,
    pub node_id: Uuid,
    pub result: Value,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct WorkerPoolError {
    pub kind: String,
    pub message: String,
}

impl WorkerPoolError {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }
}

pub trait BaseWorkerPool: Send + Sync {
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

    fn get_complete<'a>(&'a self) -> BoxFuture<'a, Vec<ActionCompletion>>;
}

pub fn error_to_value(error: &WorkerPoolError) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(error.kind.clone()));
    map.insert("message".to_string(), Value::String(error.message.clone()));
    Value::Object(map)
}
