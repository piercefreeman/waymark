//! Worker pool interface for executing actions.

use std::collections::HashMap;

use nonempty_collections::NEVec;
use serde_json::Value;
use uuid::Uuid;

/// Action execution request routed through the worker pool.
#[derive(Clone, Debug)]
pub struct ActionRequest {
    pub executor_id: Uuid,
    pub execution_id: Uuid,
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, Value>,
    pub timeout_seconds: u32,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
}

/// Completed action result emitted by the worker pool.
#[derive(Clone, Debug)]
pub struct ActionCompletion {
    pub executor_id: Uuid,
    pub execution_id: Uuid,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
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

/// Abstract worker pool with queue and batch completion polling.
pub trait BaseWorkerPool {
    /// Start any background tasks required by the pool.
    ///
    /// Default implementation is a no-op for pools that don't need launch work.
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + Send + '_ {
        async { Ok(()) }
    }

    /// Submit an action request for execution.
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

    /// Await and return a batch of completed actions, guaranteeing at least
    /// one action has completed.
    fn poll_complete(&self) -> impl Future<Output = Option<NEVec<ActionCompletion>>> + Send + '_;
}

pub fn error_to_value(error: &WorkerPoolError) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(error.kind.clone()));
    map.insert("message".to_string(), Value::String(error.message.clone()));
    Value::Object(map)
}

#[cfg(feature = "either")]
impl<Left: BaseWorkerPool, Right: BaseWorkerPool> BaseWorkerPool for either::Either<Left, Right> {
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + Send + '_ {
        match self {
            either::Either::Left(left) => either::Either::Left(left.launch()),
            either::Either::Right(right) => either::Either::Right(right.launch()),
        }
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        match self {
            either::Either::Left(left) => left.queue(request),
            either::Either::Right(right) => right.queue(request),
        }
    }

    fn poll_complete(&self) -> impl Future<Output = Option<NEVec<ActionCompletion>>> + Send + '_ {
        match self {
            either::Either::Left(left) => either::Either::Left(left.poll_complete()),
            either::Either::Right(right) => either::Either::Right(right.poll_complete()),
        }
    }
}
