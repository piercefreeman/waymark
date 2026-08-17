//! Inline worker pool that executes actions in-process.

use std::collections::HashMap;
use std::sync::Arc;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;

use waymark_observability::obs;
use waymark_worker_core::{
    ActionCompletion, ActionRequest, BaseWorkerPool, UncheckedExecutionResult, WorkerPoolError,
};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An async function serving an action call in-process.
pub type ActionCallable<Args, Ret> = Arc<dyn Fn(Args) -> BoxFuture<'static, Ret> + Send + Sync>;

/// The [`ActionCallable`] instantiation the inline worker pool serves:
/// the framing-level kwargs go to the callable untranslated, and the
/// callable answers with the completion's own result vocabulary —
/// success or exception alike, never a pool error.
pub type InlineActionCallable =
    ActionCallable<waymark_proto::messages::WorkflowArguments, UncheckedExecutionResult>;

/// Execute action requests by calling async functions in the same loop.
#[derive(Clone)]
pub struct InlineWorkerPool {
    actions: HashMap<String, InlineActionCallable>,
    sender: mpsc::Sender<ActionCompletion>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<ActionCompletion>>>,
}

impl InlineWorkerPool {
    pub fn new(actions: HashMap<String, InlineActionCallable>) -> Self {
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
        let metadata = request.metadata;
        let kwargs = request.kwargs;

        tokio::runtime::Handle::try_current().map_err(|_| {
            WorkerPoolError::new(
                "InlineWorkerPoolError",
                "inline worker pool requires an active event loop",
            )
        })?;

        tokio::spawn(async move {
            let result = handler(kwargs).await;
            let _ = sender.send(ActionCompletion { result, metadata }).await;
        });

        Ok(())
    }

    fn poll_complete(&self) -> impl Future<Output = Option<NEVec<ActionCompletion>>> {
        self.poll_complete_impl()
    }
}
