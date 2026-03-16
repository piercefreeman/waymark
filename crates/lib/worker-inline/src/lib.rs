//! Inline worker pool that executes actions in-process.

use std::collections::HashMap;
use std::sync::Arc;

use nonempty_collections::NEVec;
use serde_json::Value;
use tokio::sync::mpsc;

use waymark_observability::obs;
use waymark_worker_core::{
    ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError, error_to_value,
};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
    async fn poll_complete_impl(&self) -> Option<NEVec<ActionCompletion>> {
        let mut receiver = self.receiver.lock().await;

        let first = receiver.recv().await?;

        let mut completions = NEVec::new(first);

        while let Ok(item) = receiver.try_recv() {
            completions.push(item);
        }

        Some(completions)
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
        let attempt_number = request.attempt_number;
        let dispatch_token = request.dispatch_token;
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
                    attempt_number,
                    dispatch_token,
                    result,
                })
                .await;
        });

        Ok(())
    }

    fn get_complete<'a>(&'a self) -> BoxFuture<'a, Vec<ActionCompletion>> {
        Box::pin(async move {
            match self.poll_complete().await {
                Some(completions) => completions.into(),
                None => Vec::new(),
            }
        })
    }

    fn poll_complete(&self) -> impl Future<Output = Option<NEVec<ActionCompletion>>> {
        self.poll_complete_impl()
    }
}
