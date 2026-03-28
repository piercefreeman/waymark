mod request;
mod response;

use std::{
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nonempty_collections::NEVec;

use tokio::sync::{Mutex, mpsc};

use waymark_worker_core::{ActionCompletion, ActionRequest, WorkerPoolError, error_to_value};

async fn execute_remote_request<Spec>(
    pool: &Arc<waymark_worker_process_pool::Pool<Spec>>,
    request: ActionRequest,
) -> ActionCompletion
where
    Spec: waymark_worker_process_spec::Spec,
    Spec: Send + Sync + 'static,
{
    let executor_id = request.executor_id;
    let execution_id = request.execution_id;
    let attempt_number = request.attempt_number;
    let dispatch_token = request.dispatch_token;

    let dispatch = match request::to_dispatch_payload(request) {
        Ok(dispatch) => dispatch,
        Err(short_circuit) => return short_circuit,
    };

    let worker_idx = loop {
        if let Some(idx) = pool.try_acquire_slot() {
            break idx;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };

    let worker = pool.get_worker(worker_idx).await;

    match worker.sender.send_action(dispatch).await {
        Ok(metrics) => {
            pool.record_latency(metrics.ack_latency, metrics.worker_duration);
            pool.record_completion(worker_idx, Arc::clone(pool));
            ActionCompletion {
                executor_id,
                execution_id,
                attempt_number,
                dispatch_token,
                result: response::decode_action_result(&metrics),
            }
        }
        Err(err) => {
            pool.release_slot(worker_idx);
            ActionCompletion {
                executor_id,
                execution_id,
                attempt_number,
                dispatch_token,
                result: error_to_value(&WorkerPoolError::new(
                    "RemoteWorkerPoolError",
                    err.to_string(),
                )),
            }
        }
    }
}

// This type's only purpose is to provide transport layer to the underlying
// pool, however that poll should be itself capable of providing the said
// transport.
// TODO: move this into to `waymark-worker-message-protocol`; not done yet
// since it requires substantial changes to the code layout of the integration
// surfaces, and we want to keep things in place for review purposes.
// Another downside is the process pool wrapping requires an `Arc`, which may
// prevent proper shutdown - but without a real need for it (we only need
// to give out a tiny communication handle under an `Arc` - but that's also for
// later).
pub struct RemoteWorkerPool<Spec> {
    pool: Arc<waymark_worker_process_pool::Pool<Spec>>,
    request_tx: mpsc::Sender<ActionRequest>,
    request_rx: StdMutex<Option<mpsc::Receiver<ActionRequest>>>,
    completion_tx: mpsc::Sender<ActionCompletion>,
    completion_rx: Mutex<mpsc::Receiver<ActionCompletion>>,
    launched: AtomicBool,
}

impl<Spec> RemoteWorkerPool<Spec> {
    const DEFAULT_QUEUE_CAPACITY: usize = 1024;

    pub fn new(pool: impl Into<Arc<waymark_worker_process_pool::Pool<Spec>>>) -> Self {
        Self::with_capacity(
            pool,
            Self::DEFAULT_QUEUE_CAPACITY,
            Self::DEFAULT_QUEUE_CAPACITY,
        )
    }

    pub fn with_capacity(
        pool: impl Into<Arc<waymark_worker_process_pool::Pool<Spec>>>,
        request_capacity: usize,
        completion_capacity: usize,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(request_capacity.max(1));
        let (completion_tx, completion_rx) = mpsc::channel(completion_capacity.max(1));
        Self {
            pool: pool.into(),
            request_tx,
            request_rx: StdMutex::new(Some(request_rx)),
            completion_tx,
            completion_rx: Mutex::new(completion_rx),
            launched: AtomicBool::new(false),
        }
    }

    pub async fn shutdown_arc(
        self: Arc<Self>,
    ) -> Result<(), waymark_managed_process::ShutdownError> {
        let Some(inner) = Arc::into_inner(self) else {
            tracing::warn!(
                "remote worker pool still referenced during shutdown; skipping shutdown"
            );
            return Ok(());
        };
        inner.shutdown().await
    }

    pub async fn shutdown(self) -> Result<(), waymark_managed_process::ShutdownError> {
        self.pool.shutdown_arc().await
    }
}

impl<Spec> waymark_worker_core::BaseWorkerPool for RemoteWorkerPool<Spec>
where
    Spec: waymark_worker_process_spec::Spec,
    Spec: Send + Sync + 'static,
{
    async fn launch(&self) -> std::result::Result<(), waymark_worker_core::WorkerPoolError> {
        if self.launched.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let request_rx = {
            let mut guard = self.request_rx.lock().map_err(|_| {
                WorkerPoolError::new("RemoteWorkerPoolError", "failed to lock request receiver")
            })?;
            guard.take()
        };

        let Some(mut request_rx) = request_rx else {
            return Ok(());
        };

        let pool = Arc::clone(&self.pool);
        let completion_tx = self.completion_tx.clone();

        tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                tokio::spawn({
                    let completion_tx = completion_tx.clone();
                    let pool = Arc::clone(&pool);
                    async move {
                        let completion = execute_remote_request(&pool, request).await;
                        let _ = completion_tx.send(completion).await;
                    }
                });
            }
        });

        Ok(())
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        self.request_tx.try_send(request).map_err(|err| {
            WorkerPoolError::new(
                "RemoteWorkerPoolError",
                format!("failed to enqueue action request: {err}"),
            )
        })
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        let mut receiver = self.completion_rx.lock().await;

        let first = receiver.recv().await?;

        let mut completions = NEVec::new(first);

        while let Ok(item) = receiver.try_recv() {
            completions.push(item);
        }

        Some(completions)
    }
}

impl<Spec> waymark_worker_status_core::WorkerPoolStats for RemoteWorkerPool<Spec> {
    fn stats_snapshot(&self) -> waymark_worker_status_core::WorkerPoolStatsSnapshot {
        self.pool.stats_snapshot()
    }
}
