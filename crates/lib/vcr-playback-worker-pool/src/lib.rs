mod buffered_ingest_completion_queue;
mod completion_queue;

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;

use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

#[derive(Debug)]
pub struct Pool<ExecutionCorrelator> {
    execution_correlator: ExecutionCorrelator,
    completion_queue_ingester: buffered_ingest_completion_queue::IngestHandle,
    completion_queue_poller: tokio::sync::Mutex<buffered_ingest_completion_queue::PollHandle>,
}

impl<ExecutionCorrelator> Pool<ExecutionCorrelator> {
    pub fn new(execution_correlator: ExecutionCorrelator, ingest_buffer: NonZeroUsize) -> Self {
        let queue = self::completion_queue::CompletionQueue::default();
        let (completion_queue_ingester, completion_queue_poller) =
            self::buffered_ingest_completion_queue::new(ingest_buffer, queue);
        let completion_queue_poller = tokio::sync::Mutex::new(completion_queue_poller);
        Self {
            execution_correlator,
            completion_queue_ingester,
            completion_queue_poller,
        }
    }
}

impl<ExecutionCorrelator> BaseWorkerPool for Pool<ExecutionCorrelator>
where
    ExecutionCorrelator: waymark_vcr_playback_worker_pool_core::ExecutionCorrelator,
    ExecutionCorrelator: Sync,
    <ExecutionCorrelator as waymark_vcr_playback_worker_pool_core::ExecutionCorrelator>::Error:
        core::fmt::Display,
{
    fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError> {
        let correlated_action_completion =
            self.execution_correlator
                .correlate(request)
                .map_err(|error| {
                    WorkerPoolError::new(
                        "VcrPlaybackQueueError::ExecutionCorrelation",
                        format!("vcr playback: unable to find a correlated execution: {error}"),
                    )
                })?;

        let waymark_vcr_playback_worker_pool_core::execution_correlator::CorrelatedActionCompletion { completion, delay } =
            correlated_action_completion;

        self.completion_queue_ingester.queue(completion, delay);

        Ok(())
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        let mut completion_queue_poller = self.completion_queue_poller.lock().await;
        completion_queue_poller.poll_completions().await
    }
}
