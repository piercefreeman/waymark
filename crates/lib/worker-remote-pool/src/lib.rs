use std::{
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nonempty_collections::NEVec;

use tokio::sync::{Mutex, mpsc};

use waymark_proto::messages as proto;
use waymark_worker_core::WorkerPoolError;

async fn execute_remote_request<Spec>(
    pool: &Arc<waymark_worker_process_pool::Pool<Spec>>,
    dispatch: proto::ActionDispatch,
) -> proto::ActionResult
where
    Spec: waymark_worker_process_spec::Spec,
    Spec: Send + Sync + 'static,
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
            pool.record_latency(metrics.ack_latency, metrics.worker_duration);
            pool.record_completion(worker_idx, Arc::clone(pool));
            proto::ActionResult {
                payload: metrics.response_payload,
                metadata,
                ..Default::default()
            }
        }
        Err(err) => {
            pool.release_slot(worker_idx);
            proto::ActionResult {
                payload: waymark_proto_python_value_conversions::encode_action_result_value(
                    &waymark_proto_python_value_conversions::raised_exception(
                        waymark_proto_python_value_conversions::exception_value(
                            "RemoteWorkerPoolError".to_owned(),
                            err.to_string(),
                        ),
                    ),
                ),
                metadata,
                ..Default::default()
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
    request_tx: mpsc::Sender<proto::ActionDispatch>,
    request_rx: StdMutex<Option<mpsc::Receiver<proto::ActionDispatch>>>,
    completion_tx: mpsc::Sender<proto::ActionResult>,
    completion_rx: Mutex<mpsc::Receiver<proto::ActionResult>>,
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

        // Start a background loop to handle the `ActionRequest`s coming
        // through the `request_rx`: serve each of them independently
        // (each in their own background task) via `execute_remote_request`
        // and, finally, send the completion over to the pool for polling.
        tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                tokio::spawn({
                    let completion_tx = completion_tx.clone();
                    let pool = Arc::clone(&pool);
                    async move {
                        let before = std::time::Instant::now();

                        let completion = execute_remote_request(&pool, request).await;

                        metrics::histogram!("waymark_worker_remote_execute_remote_request_seconds")
                            .record(before.elapsed());

                        let _ = completion_tx.send(completion).await;
                    }
                });
            }
        });

        Ok(())
    }

    fn queue(&self, dispatch: proto::ActionDispatch) -> Result<(), WorkerPoolError> {
        self.request_tx.try_send(dispatch).map_err(|err| {
            WorkerPoolError::new(
                "RemoteWorkerPoolError",
                format!("failed to enqueue action request: {err}"),
            )
        })
    }

    async fn poll_complete(&self) -> Option<NEVec<proto::ActionResult>> {
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
