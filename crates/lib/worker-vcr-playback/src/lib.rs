use nonempty_collections::NEVec;

use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

#[derive(Clone)]
pub struct Playback {
    pub index: (),
}

impl BaseWorkerPool for Playback {
    async fn launch(&self) -> Result<(), WorkerPoolError> {
        Ok(())
    }

    fn queue(&self, _request: ActionRequest) -> Result<(), WorkerPoolError> {
        todo!()
    }

    async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>> {
        todo!()
    }
}
