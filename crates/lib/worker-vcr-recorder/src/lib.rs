use nonempty_collections::NEVec;

use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

#[derive(Clone)]
pub struct Recorder<Pool> {
    pub pool: Pool,
}

impl<Pool> BaseWorkerPool for Recorder<Pool>
where
    Pool: BaseWorkerPool,
    Pool: Sync,
{
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + '_ {
        self.pool.launch()
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        tracing::info!(?request, message = "enqueue");
        self.pool.queue(request)
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        self.pool
            .poll_complete()
            .await
            .inspect(|completions| tracing::info!(?completions, message = "poll_complete"))
    }
}
