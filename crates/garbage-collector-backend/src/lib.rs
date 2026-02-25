use chrono::{DateTime, Utc};

pub use waymark_backends_core::{BackendError, BackendResult};

#[derive(Clone, Copy, Debug, Default)]
/// Summary of a garbage collection sweep.
pub struct GarbageCollectionResult {
    pub deleted_instances: usize,
    pub deleted_actions: usize,
}

/// Backend capability for deleting old finished workflow data.
#[async_trait::async_trait]
pub trait GarbageCollectorBackend: Send + Sync {
    async fn collect_done_instances(
        &self,
        older_than: DateTime<Utc>,
        limit: usize,
    ) -> BackendResult<GarbageCollectionResult>;
}
