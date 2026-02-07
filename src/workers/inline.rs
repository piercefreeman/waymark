//! Inline worker pool that executes actions in-process.

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde_json::Value;
use tokio::sync::mpsc;

use super::base::{
    ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError, error_to_value,
};
use crate::observability::obs;

pub type ActionCallable = Arc<
    dyn Fn(HashMap<String, Value>) -> BoxFuture<'static, Result<Value, WorkerPoolError>>
        + Send
        + Sync,
>;

/// Execute action requests by calling async functions in the same loop.
#[derive(Clone)]
pub struct InlineWorkerPool {
    actions: HashMap<String, ActionCallable>,
    sender: mpsc::Sender<ActionCompletion>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ActionCompletion>>>,
}

impl InlineWorkerPool {
    pub fn new(actions: HashMap<String, ActionCallable>) -> Self {
        let (sender, receiver) = mpsc::channel(256);
        Self {
            actions,
            sender,
            receiver: Arc::new(tokio::sync::Mutex::new(receiver)),
        }
    }

    #[obs]
    async fn get_complete_impl(&self) -> Vec<ActionCompletion> {
        let mut receiver = self.receiver.lock().await;
        let mut completions = Vec::new();
        match receiver.recv().await {
            Some(first) => completions.push(first),
            None => return completions,
        }
        while let Ok(value) = receiver.try_recv() {
            completions.push(value);
        }
        completions
    }
}

impl BaseWorkerPool for InlineWorkerPool {
    #[obs]
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        let handler = self
            .actions
            .get(&request.action_name)
            .cloned()
            .ok_or_else(|| {
                WorkerPoolError::new(
                    "InlineWorkerPoolError",
                    format!("unknown action: {}", request.action_name),
                )
            })?;

        let sender = self.sender.clone();
        let executor_id = request.executor_id;
        let execution_id = request.execution_id;
        let kwargs = request.kwargs;

        tokio::runtime::Handle::try_current().map_err(|_| {
            WorkerPoolError::new(
                "InlineWorkerPoolError",
                "inline worker pool requires an active event loop",
            )
        })?;

        tokio::spawn(async move {
            let result = match handler(kwargs).await {
                Ok(value) => value,
                Err(err) => error_to_value(&err),
            };
            let _ = sender
                .send(ActionCompletion {
                    executor_id,
                    execution_id,
                    result,
                })
                .await;
        });

        Ok(())
    }

    fn get_complete<'a>(&'a self) -> BoxFuture<'a, Vec<ActionCompletion>> {
        Box::pin(self.get_complete_impl())
    }
}
