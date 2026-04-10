use nonempty_collections::NEVec;

use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

#[derive(Debug)]
pub struct Pool<InnerPool> {
    pub inner: InnerPool,
    pub recorder: waymark_vcr_recorder::pool::Handle,
}

impl<InnerPool> BaseWorkerPool for Pool<InnerPool>
where
    InnerPool: BaseWorkerPool,
    InnerPool: Sync,
{
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + '_ {
        self.inner.launch()
    }

    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        tracing::info!(?request, message = "enqueue");
        if let Err(error) = self.recorder.request(request.clone()) {
            tracing::warn!(message = "unable to record action request", ?error);
        }
        self.inner.queue(request)
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        let completions = self.inner.poll_complete().await?;

        for completion in &completions {
            if let Err(error) = self.recorder.completion(completion.clone()) {
                tracing::warn!(message = "unable to record completion", ?error);
                break;
            }
        }

        Some(completions)
    }
}
