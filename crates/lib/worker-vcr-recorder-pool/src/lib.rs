use nonempty_collections::NEVec;

use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

#[derive(Debug)]
pub struct Pool<WrappedPool> {
    pub wrapped: WrappedPool,
    pub recorder: waymark_worker_vcr_recorder::Handle,
}

impl<WrappedPool> BaseWorkerPool for Pool<WrappedPool>
where
    WrappedPool: BaseWorkerPool,
    WrappedPool: Sync,
{
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + '_ {
        self.wrapped.launch()
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        tracing::info!(?request, message = "enqueue");
        self.wrapped.queue(request)
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        self.wrapped
            .poll_complete()
            .await
            .inspect(|completions| tracing::info!(?completions, message = "poll_complete"))
    }
}
