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

const DEFAULT_QUEUE_CAPACITY: usize = 256;

/// A dispatch with its handler resolved: what the queue carries to the
/// worker pool loop.
struct Request {
    handler: InlineActionCallable,
    dispatch: proto::ActionDispatch,
}

/// The worker pool handle: the dispatch queue in, the completion queue
/// out.
///
/// Obtained from [`run`]. Dropping every worker pool handle closes the
/// request queue, which is what lets the worker pool loop finish once
/// every action it took has completed.
pub struct Pool {
    actions: HashMap<String, InlineActionCallable>,
    request_tx: mpsc::Sender<Request>,
    completion_rx: tokio::sync::Mutex<mpsc::Receiver<ActionExecutionReport>>,
}

/// The worker pool handle and the worker pool loop serving `actions`.
///
/// The loop runs only once awaited or spawned by the caller. It owns the
/// actions it serves, each run as its own task, and ends once every
/// [`Pool`] handle is dropped and every action it took has completed. If
/// the loop is dropped instead, the actions in flight are aborted with
/// it: nothing of the pool's outlives the pool.
pub fn run(
    actions: HashMap<String, InlineActionCallable>,
) -> (
    Pool,
    impl Future<Output = Result<(), std::convert::Infallible>> + Send + 'static,
) {
    let (request_tx, request_rx) = mpsc::channel(DEFAULT_QUEUE_CAPACITY);
    let (completion_tx, completion_rx) = mpsc::channel(DEFAULT_QUEUE_CAPACITY);

    let pool_loop = pool_loop(request_rx, completion_tx);

    let pool = Pool {
        actions,
        request_tx,
        completion_rx: tokio::sync::Mutex::new(completion_rx),
    };

    (pool, pool_loop)
}

async fn pool_loop(
    mut request_rx: mpsc::Receiver<Request>,
    completion_tx: mpsc::Sender<ActionExecutionReport>,
) -> Result<(), std::convert::Infallible> {
    let mut in_flight = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            Some(Request { handler, dispatch }) = request_rx.recv() => {
                let completion_tx = completion_tx.clone();
                in_flight.spawn(serve(handler, dispatch, completion_tx));
            }
            Some(joined) = in_flight.join_next() => {
                if let Err(join_error) = joined {
                    tracing::error!(%join_error, "an inline action panicked");
                }
            }
            else => break,
        }
    }

    Ok(())
}

/// Serve one dispatch and deliver its completion to the completion queue.
async fn serve(
    handler: InlineActionCallable,
    dispatch: proto::ActionDispatch,
    completion_tx: mpsc::Sender<ActionExecutionReport>,
) {
    let payload = handler(dispatch.arguments).await;
    let result = proto::ActionResult {
        payload,
        metadata: dispatch.metadata,
        ..Default::default()
    };

    // An in-process action always finishes by completing: the body runs
    // to an outcome right here, so there is no worker to lose.
    let _ = completion_tx
        .send(ActionExecutionReport::Completed(result))
        .await;
}

impl Pool {
    #[obs]
    async fn poll_complete_impl(
        &self,
    ) -> Result<NEVec<ActionExecutionReport>, WorkerPoolGoneError> {
        let mut receiver = self.completion_rx.lock().await;

        let first = receiver.recv().await.ok_or(WorkerPoolGoneError)?;

        let mut executions = NEVec::new(first);

        while let Ok(item) = receiver.try_recv() {
            executions.push(item);
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

    /// The request queue is full: the worker pool loop has not taken the
    /// earlier dispatches yet.
    #[error("inline worker pool request queue is full")]
    Full,

    /// The worker pool loop is gone: it serves nothing further.
    #[error("inline worker pool request queue is closed")]
    Closed,
}

impl waymark_worker_core::QueueActionDispatch for Pool {
    type Error = QueueError;

    #[obs]
    async fn queue(&self, dispatch: proto::ActionDispatch) -> Result<(), Self::Error> {
        let Some(handler) = self.actions.get(&dispatch.action_name).cloned() else {
            return Err(QueueError::UnknownAction {
                action_name: dispatch.action_name,
            });
        };

        self.request_tx
            .try_send(Request { handler, dispatch })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => QueueError::Full,
                mpsc::error::TrySendError::Closed(_) => QueueError::Closed,
            })
    }
}

impl waymark_worker_core::PollActionResults for Pool {
    type Error = WorkerPoolGoneError;

    fn poll_complete(
        &self,
    ) -> impl Future<Output = Result<NEVec<ActionExecutionReport>, Self::Error>> {
        self.poll_complete_impl()
    }
}
