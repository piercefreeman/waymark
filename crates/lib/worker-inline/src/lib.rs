//! Inline worker pool that executes actions in-process.

use std::collections::HashMap;
use std::sync::Arc;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;

use waymark_observability::obs;
use waymark_proto::messages as proto;
use waymark_worker_core::{ActionExecutionReport, WorkerPoolGoneError};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// An async function serving an action call in-process.
pub type ActionCallable<Args, Ret> = Arc<dyn Fn(Args) -> BoxFuture<'static, Ret> + Send + Sync>;

/// The [`ActionCallable`] instantiation the inline worker pool serves:
/// the dispatch's opaque encoded arguments go to the callable
/// untranslated, and the callable returns the encoded result payload —
/// success and failure discriminated structurally, never a pool error.
/// The pool stamps the correlation metadata; the callable leaves it
/// alone.
pub type InlineActionCallable = ActionCallable<Vec<u8>, Vec<u8>>;

/// Execute action requests by calling async functions in the same loop.
#[derive(Clone)]
pub struct InlineWorkerPool {
    actions: HashMap<String, InlineActionCallable>,
    sender: mpsc::Sender<proto::ActionResult>,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<proto::ActionResult>>>,
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
    async fn poll_complete_impl(
        &self,
    ) -> Result<NEVec<ActionExecutionReport>, WorkerPoolGoneError> {
        let mut receiver = self.receiver.lock().await;

        let first = receiver.recv().await.ok_or(WorkerPoolGoneError)?;

        // An in-process action always finishes by completing: the body
        // runs to an outcome right here, so there is no worker to lose.
        let mut executions = NEVec::new(ActionExecutionReport::Completed(first));

        while let Ok(item) = receiver.try_recv() {
            executions.push(ActionExecutionReport::Completed(item));
        }

        Ok(executions)
    }
}

/// Error from queueing an action dispatch on the inline worker pool.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// No handler is registered under the dispatched action name.
    #[error("unknown action: {action_name}")]
    UnknownAction {
        /// The name the dispatch asked for.
        action_name: String,
    },

    /// Handlers run as tokio tasks, so a runtime has to be current.
    #[error("inline worker pool requires an active tokio runtime: {0}")]
    NoRuntime(#[source] tokio::runtime::TryCurrentError),
}

impl waymark_worker_core::QueueActionDispatch for InlineWorkerPool {
    type Error = QueueError;

    #[obs]
    async fn queue(&self, dispatch: proto::ActionDispatch) -> Result<(), Self::Error> {
        let handler = self
            .actions
            .get(&dispatch.action_name)
            .cloned()
            .ok_or_else(|| QueueError::UnknownAction {
                action_name: dispatch.action_name.clone(),
            })?;

        let sender = self.sender.clone();
        let metadata = dispatch.metadata;
        let arguments = dispatch.arguments;

        tokio::runtime::Handle::try_current().map_err(QueueError::NoRuntime)?;

        tokio::spawn(async move {
            let payload = handler(arguments).await;
            let result = proto::ActionResult {
                payload,
                metadata,
                ..Default::default()
            };
            let _ = sender.send(result).await;
        });

        Ok(())
    }
}

impl waymark_worker_core::PollActionResults for InlineWorkerPool {
    type Error = WorkerPoolGoneError;

    fn poll_complete(
        &self,
    ) -> impl Future<Output = Result<NEVec<ActionExecutionReport>, Self::Error>> {
        self.poll_complete_impl()
    }
}
