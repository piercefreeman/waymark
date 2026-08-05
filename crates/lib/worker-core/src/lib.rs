//! Worker pool interface for executing actions.

use std::collections::HashMap;

use nonempty_collections::NEVec;
use serde_json::Value;
use uuid::Uuid;
use waymark_ids::{ExecutionId, InstanceId};

/// Action execution request routed through the worker pool.
#[derive(Clone, Debug)]
pub struct ActionRequest {
    pub executor_id: InstanceId,
    pub execution_id: ExecutionId,
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, Value>,
    pub timeout_seconds: u32,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
    pub metadata: Vec<u8>,
}

/// Completed action result emitted by the worker pool.
#[derive(Clone, Debug)]
pub struct ActionCompletion {
    pub executor_id: InstanceId,
    pub execution_id: ExecutionId,
    pub attempt_number: u32,
    pub dispatch_token: Uuid,
    pub result: UncheckedExecutionResult,
    pub metadata: Vec<u8>,
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

pub fn error_to_value(error: &WorkerPoolError) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert("type".to_string(), Value::String(error.kind.clone()));
    map.insert("message".to_string(), Value::String(error.message.clone()));
    Value::Object(map)
}

impl<T> BaseWorkerPool for std::sync::Arc<T>
where
    T: BaseWorkerPool + Send + Sync,
{
    async fn launch(&self) -> Result<(), WorkerPoolError> {
        (**self).launch().await
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        (**self).queue(request)
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        (**self).poll_complete().await
    }
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

/// An unchecked execution result.
///
/// Use when you have some execution result, but you didn't yet check what kind
/// of result it is (as in success vs exception).
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct UncheckedExecutionResult(pub serde_json::Value);

/// A successful execution result.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ExecutionSuccess(pub serde_json::Value);

/// A failed execution result.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct ExecutionException(pub serde_json::Value);

/// An checked execution result.
///
/// Use when you have some execution result that you have already checked
/// (and know what kind it is) when you need to allow both success and failure
/// through a certain codepath while retaining the knowledge of which one it is.
pub type CheckedExecutionResult = Result<ExecutionSuccess, ExecutionException>;

impl UncheckedExecutionResult {
    /// Look and the underlying JSON of the unchecked execution result and
    /// determine from it whether it is a success or an exception.
    pub fn check(self) -> CheckedExecutionResult {
        if is_exception_value(&self.0) {
            return Err(ExecutionException(self.0));
        }
        Ok(ExecutionSuccess(self.0))
    }

    /// Unwrap the inner JSON value.
    pub fn into_json(self) -> serde_json::Value {
        self.0
    }
}

/// Go from a checked execution result to an unchecked one, essentially
/// erasing the type information on the result kind.
pub fn uncheck_execution_result(checked: CheckedExecutionResult) -> UncheckedExecutionResult {
    UncheckedExecutionResult(match checked {
        Ok(result) => result.0,
        Err(result) => result.0,
    })
}

impl From<ExecutionSuccess> for CheckedExecutionResult {
    fn from(value: ExecutionSuccess) -> Self {
        Ok(value)
    }
}

impl From<ExecutionException> for CheckedExecutionResult {
    fn from(value: ExecutionException) -> Self {
        Err(value)
    }
}

impl ExecutionSuccess {
    /// Go from an execution success to an unchecked execution result,
    /// essentially erasing the type information on the result kind.
    pub fn into_unchecked(self) -> UncheckedExecutionResult {
        UncheckedExecutionResult(self.0)
    }
}

impl ExecutionException {
    /// Go from an execution exception to an unchecked execution result,
    /// essentially erasing the type information on the result kind.
    pub fn into_unchecked(self) -> UncheckedExecutionResult {
        UncheckedExecutionResult(self.0)
    }
}

/// Determine whether the given JSON value has the shape of a worker-reported
/// exception.
pub fn is_exception_value(value: &serde_json::Value) -> bool {
    if let serde_json::Value::Object(map) = &value {
        return map.contains_key("type") && map.contains_key("message");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_exception_value_happy_path() {
        let value = serde_json::json!({
            "type": "RuntimeError",
            "message": "bad",
        });
        assert!(is_exception_value(&value));
    }
}
