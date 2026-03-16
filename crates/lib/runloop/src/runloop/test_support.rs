use std::future::Future;
use std::pin::Pin;

use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

mockall::mock! {
    pub WorkerPool {}

    impl BaseWorkerPool for WorkerPool {
        fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

        fn get_complete<'a>(
            &'a self,
        ) -> Pin<Box<dyn Future<Output = Vec<ActionCompletion>> + Send + 'a>>;

        fn poll_complete<'a>(
            &'a self,
        ) -> impl Future<Output = Option<nonempty_collections::NEVec<ActionCompletion>>> + Send + Sync + 'a;
    }
}

pub fn assert_no_extra_worker_pool_calls(worker_pool: &mut MockWorkerPool) {
    worker_pool.checkpoint();
}
