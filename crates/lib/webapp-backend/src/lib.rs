use std::future::Future;

use waymark_backends_core::BackendResult;
use waymark_webapp_core::WorkerStatus;

/// Backend capability for webapp-specific queries.
pub trait WebappBackend {
    fn get_worker_statuses(
        &self,
        window_minutes: i64,
    ) -> impl Future<Output = BackendResult<Vec<WorkerStatus>>> + Send + '_;
}
