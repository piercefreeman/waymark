//! The remote worker pool: the worker-core traits over a worker process
//! pool, with the worker process pool owned by the worker pool loop.

use std::time::Duration;

use nonempty_collections::NEVec;

use tokio::sync::mpsc;

use waymark_proto::messages as proto;
use waymark_worker_core::{
    ActionExecutionLoss, ActionExecutionReport, ExecutionProgress, WorkerPoolError,
    WorkerPoolGoneError,
};

const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// The handle to the worker pool loop: the request queue in, the
/// completion queue out.
///
/// Obtained from [`run`]. Dropping every worker pool handle closes the
/// request queue, which is what lets the worker pool loop finish and shut
/// the worker process pool down.
#[derive(Debug)]
pub struct Pool {
    request_tx: mpsc::Sender<proto::ActionDispatch>,
    completion_rx: tokio::sync::Mutex<mpsc::Receiver<ActionExecutionReport>>,
}

/// The worker pool loop over the worker process pool `pool`, with the
/// default queue capacities; see [`run_with_capacity`].
pub fn run<Spec>(
    pool: waymark_worker_process_pool::Pool<Spec>,
) -> (
    Pool,
    impl Future<Output = Result<(), waymark_managed_process::ShutdownError>> + Send + 'static,
)
where
    Spec: waymark_worker_process_spec::Spec,
    Spec: Send + Sync + 'static,
{
    run_with_capacity(pool, DEFAULT_QUEUE_CAPACITY, DEFAULT_QUEUE_CAPACITY)
}

/// The worker pool handle and the worker pool loop over the worker process
/// pool `pool`, which the loop owns from here on.
///
/// The loop runs only once awaited or spawned by the caller. It ends once
/// every [`Pool`] handle is dropped and every request it took
/// has been served; it then shuts the worker process pool down and ends
/// with the result.
pub fn run_with_capacity<Spec>(
    pool: waymark_worker_process_pool::Pool<Spec>,
    request_capacity: usize,
    completion_capacity: usize,
) -> (
    Pool,
    impl Future<Output = Result<(), waymark_managed_process::ShutdownError>> + Send + 'static,
)
where
    Spec: waymark_worker_process_spec::Spec,
    Spec: Send + Sync + 'static,
{
    let (request_tx, request_rx) = mpsc::channel(request_capacity.max(1));
    let (completion_tx, completion_rx) = mpsc::channel(completion_capacity.max(1));

    let pool_loop = pool_loop(pool, request_rx, completion_tx);

    let handle = Pool {
        request_tx,
        completion_rx: tokio::sync::Mutex::new(completion_rx),
    };

    (handle, pool_loop)
}

/// The worker pool loop: serves every request off the queue as a future
/// borrowed from the worker process pool, runs the recycles those
/// completions make due, and, with the queue closed and nothing in flight,
/// shuts the worker process pool down.
async fn pool_loop<Spec>(
    pool: waymark_worker_process_pool::Pool<Spec>,
    mut request_rx: mpsc::Receiver<proto::ActionDispatch>,
    completion_tx: mpsc::Sender<ActionExecutionReport>,
) -> Result<(), waymark_managed_process::ShutdownError>
where
    Spec: waymark_worker_process_spec::Spec,
{
    use futures_util::StreamExt as _;

    let mut in_flight = futures_util::stream::FuturesUnordered::new();
    let mut recycles = futures_util::stream::FuturesUnordered::new();
    let mut intake_open = true;

    loop {
        tokio::select! {
            received = request_rx.recv(), if intake_open => match received {
                Some(dispatch) => {
                    record_dispatch_queue_length(request_rx.len());
                    in_flight.push(serve(&pool, dispatch, &completion_tx));
                }
                None => intake_open = false,
            },
            // An empty set yields `None` at once, so each is polled only
            // while it holds something; with the intake closed as well,
            // there is nothing left to wait for.
            Some(recycle_due) = in_flight.next(), if !in_flight.is_empty() => {
                if let Some(worker_idx) = recycle_due {
                    recycles.push(recycle(&pool, worker_idx));
                }
            }
            Some(()) = recycles.next(), if !recycles.is_empty() => {}
            else => break,
        }
    }

    drop(in_flight);
    drop(recycles);

    pool.shutdown().await
}

/// Serve one request and deliver its report to the completion queue;
/// returns the worker index when the completion made a recycle due.
async fn serve<Spec>(
    pool: &waymark_worker_process_pool::Pool<Spec>,
    dispatch: proto::ActionDispatch,
    completion_tx: &mpsc::Sender<ActionExecutionReport>,
) -> Option<usize>
where
    Spec: waymark_worker_process_spec::Spec,
{
    let before = std::time::Instant::now();

    let (report, recycle_due) = execute_remote_request(pool, dispatch).await;

    metrics::histogram!("waymark_worker_remote_execute_remote_request_seconds")
        .record(before.elapsed());

    let _ = completion_tx.send(report).await;

    recycle_due
}

async fn recycle<Spec>(pool: &waymark_worker_process_pool::Pool<Spec>, worker_idx: usize)
where
    Spec: waymark_worker_process_spec::Spec,
{
    if let Err(err) = pool.recycle_worker(worker_idx).await {
        tracing::error!(worker_idx, ?err, "failed to recycle worker");
    }
}

async fn execute_remote_request<Spec>(
    pool: &waymark_worker_process_pool::Pool<Spec>,
    dispatch: proto::ActionDispatch,
) -> (ActionExecutionReport, Option<usize>)
where
    Spec: waymark_worker_process_spec::Spec,
{
    let metadata = dispatch.metadata.clone();

    let before = std::time::Instant::now();
    let worker_idx = loop {
        if let Some(idx) = pool.try_acquire_slot() {
            break idx;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    metrics::histogram!("waymark_worker_remote_execute_remote_request_worker_wait_seconds")
        .record(before.elapsed());

    let sender = pool.get_worker_sender(worker_idx).await;

    let before = std::time::Instant::now();
    let result = sender.send_action(dispatch).await;
    metrics::histogram!("waymark_worker_remote_send_action_seconds").record(before.elapsed());

    match result {
        Ok(metrics) => {
            ::metrics::histogram!("waymark_worker_remote_pool_action_handling_seconds")
                .record(metrics.worker_duration);
            let recycle_due = pool
                .record_completion(worker_idx)
                .map(|waymark_worker_process_pool::RecycleDue| worker_idx);
            let report = ActionExecutionReport::Completed(proto::ActionResult {
                payload: metrics.response_payload,
                metadata,
                ..Default::default()
            });
            (report, recycle_due)
        }
        Err(err) => {
            pool.release_slot(worker_idx);
            // The worker died on us: report the loss as the fact it is.
            // The pool decides nothing here — what a lost execution means
            // for the awaiting promise is the VM's business.
            let progress = match err {
                // The worker protocol was already closed before the
                // dispatch was registered: the action provably never
                // started.
                waymark_worker_message_protocol::SendActionError::WorkerProtocolClosed => {
                    ExecutionProgress::NotStarted
                }
                // The channel closed somewhere past registration: the
                // action may not have run at all, or may have run to
                // completion with only the result lost.
                waymark_worker_message_protocol::SendActionError::ChannelClosed => {
                    ExecutionProgress::Unknown
                }
            };
            (
                ActionExecutionReport::Lost(ActionExecutionLoss { metadata, progress }),
                None,
            )
        }
    }
}

/// Publish the number of action requests waiting in the dispatch queue —
/// the queue between `queue` and the worker pool loop that hands them to
/// workers.
fn record_dispatch_queue_length(queued: usize) {
    metrics::gauge!("waymark_worker_remote_pool_dispatch_queue_length").set(queued as f64);
}

impl waymark_worker_core::QueueActionDispatch for Pool {
    type Error = WorkerPoolError;

    async fn queue(&self, dispatch: proto::ActionDispatch) -> Result<(), Self::Error> {
        self.request_tx.try_send(dispatch).map_err(|err| {
            WorkerPoolError::new(
                "RemoteWorkerPoolError",
                format!("failed to enqueue action request: {err}"),
            )
        })?;
        record_dispatch_queue_length(
            self.request_tx
                .max_capacity()
                .saturating_sub(self.request_tx.capacity()),
        );
        Ok(())
    }
}

impl waymark_worker_core::PollActionResults for Pool {
    type Error = WorkerPoolGoneError;

    async fn poll_complete(&self) -> Result<NEVec<ActionExecutionReport>, Self::Error> {
        let mut receiver = self.completion_rx.lock().await;

        let first = receiver.recv().await.ok_or(WorkerPoolGoneError)?;

        let mut completions = NEVec::new(first);

        while let Ok(item) = receiver.try_recv() {
            completions.push(item);
        }

        Ok(completions)
    }
}
