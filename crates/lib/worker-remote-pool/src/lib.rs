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

use waymark_runner_executor_core::UncheckedExecutionResult;
use waymark_worker_core::{ActionCompletion, ActionRequest, WorkerPoolError, error_to_value};

/// Routes action calls to the worker pool for their declared runtime.
pub struct RuntimeWorkerPool<PythonPool, JavaScriptPool> {
    python: PythonPool,
    javascript: Option<JavaScriptPool>,
}

impl<PythonPool, JavaScriptPool> RuntimeWorkerPool<PythonPool, JavaScriptPool> {
    /// Create a runtime-routed worker pool.
    pub fn new(python: PythonPool, javascript: Option<JavaScriptPool>) -> Self {
        Self { python, javascript }
    }
}

impl<PythonPool, JavaScriptPool> waymark_worker_core::BaseWorkerPool
    for RuntimeWorkerPool<PythonPool, JavaScriptPool>
where
    PythonPool: waymark_worker_core::BaseWorkerPool + Send + Sync,
    JavaScriptPool: waymark_worker_core::BaseWorkerPool + Send + Sync,
{
    async fn launch(&self) -> Result<(), WorkerPoolError> {
        self.python.launch().await?;
        if let Some(javascript) = &self.javascript {
            javascript.launch().await?;
        }
        Ok(())
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        match request.runtime {
            waymark_action_core::ActionRuntime::Python => self.python.queue(request),
            waymark_action_core::ActionRuntime::JavaScript => {
                let Some(javascript) = &self.javascript else {
                    return Err(WorkerPoolError::new(
                        "ActionRuntimeUnavailable",
                        "no JavaScript worker pool is configured",
                    ));
                };
                javascript.queue(request)
            }
        }
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        let Some(javascript) = &self.javascript else {
            return self.python.poll_complete().await;
        };

        tokio::select! {
            completions = self.python.poll_complete() => completions,
            completions = javascript.poll_complete() => completions,
        }
    }
}

impl<PythonPool, JavaScriptPool> waymark_worker_status_core::WorkerPoolStats
    for RuntimeWorkerPool<PythonPool, JavaScriptPool>
where
    PythonPool: waymark_worker_status_core::WorkerPoolStats,
    JavaScriptPool: waymark_worker_status_core::WorkerPoolStats,
{
    fn stats_snapshot(&self) -> waymark_worker_status_core::WorkerPoolStatsSnapshot {
        let python = self.python.stats_snapshot();
        let Some(javascript) = &self.javascript else {
            return python;
        };
        let javascript = javascript.stats_snapshot();

        waymark_worker_status_core::WorkerPoolStatsSnapshot {
            active_workers: python
                .active_workers
                .saturating_add(javascript.active_workers),
            throughput_per_min: python.throughput_per_min + javascript.throughput_per_min,
            total_completed: python
                .total_completed
                .saturating_add(javascript.total_completed),
            last_action_at: python.last_action_at.max(javascript.last_action_at),
            dispatch_queue_size: python
                .dispatch_queue_size
                .saturating_add(javascript.dispatch_queue_size),
            total_in_flight: python
                .total_in_flight
                .saturating_add(javascript.total_in_flight),
            median_dequeue_ms: python.median_dequeue_ms.max(javascript.median_dequeue_ms),
            median_handling_ms: python.median_handling_ms.max(javascript.median_handling_ms),
        }
    }
}

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
    let metadata = request.metadata.clone();

    let dispatch = match request::to_dispatch_payload(request) {
        Ok(dispatch) => dispatch,
        Err(short_circuit) => return short_circuit,
    };

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
            ActionCompletion {
                executor_id,
                execution_id,
                attempt_number,
                dispatch_token,
                result: UncheckedExecutionResult(response::decode_action_result(&metrics)),
                metadata,
            }
        }
        Err(err) => {
            pool.release_slot(worker_idx);
            ActionCompletion {
                executor_id,
                execution_id,
                attempt_number,
                dispatch_token,
                result: UncheckedExecutionResult(error_to_value(&WorkerPoolError::new(
                    "RemoteWorkerPoolError",
                    err.to_string(),
                ))),
                metadata,
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use waymark_worker_core::BaseWorkerPool as _;

    use super::*;

    #[derive(Default)]
    struct RecordingPool {
        requests: StdMutex<Vec<ActionRequest>>,
    }

    impl waymark_worker_core::BaseWorkerPool for RecordingPool {
        fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
            self.requests.lock().unwrap().push(request);
            Ok(())
        }

        async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
            std::future::pending().await
        }
    }

    fn request(runtime: waymark_action_core::ActionRuntime) -> ActionRequest {
        ActionRequest {
            runtime,
            executor_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            execution_id: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
            action_name: "act".to_owned(),
            module_name: None,
            kwargs: Default::default(),
            timeout_seconds: 1,
            attempt_number: 1,
            dispatch_token: "00000000-0000-0000-0000-000000000003".parse().unwrap(),
            metadata: Vec::new(),
        }
    }

    #[test]
    fn routes_requests_by_runtime() {
        let pool = RuntimeWorkerPool::new(RecordingPool::default(), Some(RecordingPool::default()));

        pool.queue(request(waymark_action_core::ActionRuntime::Python))
            .unwrap();
        pool.queue(request(waymark_action_core::ActionRuntime::JavaScript))
            .unwrap();

        assert_eq!(pool.python.requests.lock().unwrap().len(), 1);
        assert_eq!(
            pool.javascript
                .as_ref()
                .unwrap()
                .requests
                .lock()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn classifies_an_unconfigured_javascript_runtime() {
        let pool = RuntimeWorkerPool::<_, RecordingPool>::new(RecordingPool::default(), None);

        let error = pool
            .queue(request(waymark_action_core::ActionRuntime::JavaScript))
            .unwrap_err();

        assert_eq!(error.kind, "ActionRuntimeUnavailable");
    }
}
