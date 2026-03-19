//! Worker interface for executing actions.

use std::collections::HashMap;

use nonempty_collections::NEVec;
use serde_json::Value;
use uuid::Uuid;

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
pub trait BaseWorkerPool: Send + Sync {
    /// Start any background tasks required by the pool.
    ///
    /// Default implementation is a no-op for pools that don't need launch work.
    fn launch<'a>(&'a self) -> BoxFuture<'a, Result<(), WorkerPoolError>> {
        Box::pin(async { Ok(()) })
    }

    /// Submit an action request for execution.
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

    /// Await and return a batch of completed actions, guaranteeing at least
    /// one action has completed.
    fn poll_complete(
        &self,
    ) -> impl Future<Output = Option<NEVec<ActionCompletion>>> + Send + Sync + '_;
}

pub fn error_to_value(error: &WorkerPoolError) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(error.kind.clone()));
    map.insert("message".to_string(), Value::String(error.message.clone()));
    Value::Object(map)
}

/// A worker launcher.
pub trait Launcher {
    /// The handle to the running worker.
    ///
    /// This type is expected to provide all necessary traits that we demand
    /// from a worker by either implementing them directly.
    type Handle;

    /// The worker execution future.
    ///
    /// The worker is expected to abort the execution when this future
    /// is dropped.
    type Execution: Future;

    /// The error that can occur while launching.
    type Error;

    /// Start the worker with the given cancellation token.
    ///
    /// The worker is expected to gracefully stop upon receivieng the token
    /// cancellation.
    fn launch(
        &self,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> impl Future<Output = Result<(Self::Handle, Self::Execution), Self::Error>> + Send + '_;
}

/// Enqueue action onto a worker for execution.
pub trait EnqueueAction {
    /// A error than can occur while enqeuing the action onto the worker.
    type Error;

    /// Submit an action request for execution.
    fn enqueue(
        &self,
        request: ActionRequest,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + '_;
}

/// Poll the worker for action completion notifications.
pub trait PollActionCompletion {
    /// A error than can occur while polling the worker for
    /// an action completion.
    type Error;

    /// Await and return a batch of completed actions.
    fn poll_complete(
        &self,
    ) -> impl Future<Output = Result<NEVec<ActionCompletion>, Self::Error>> + Send + '_;
}
