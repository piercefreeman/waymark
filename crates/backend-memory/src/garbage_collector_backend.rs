use chrono::{DateTime, Utc};
use waymark_garbage_collector_backend::{
    BackendResult, GarbageCollectionResult, GarbageCollectorBackend,
};

#[async_trait::async_trait]
impl GarbageCollectorBackend for crate::MemoryBackend {
    async fn collect_done_instances(
        &self,
        _older_than: DateTime<Utc>,
        _limit: usize,
    ) -> BackendResult<GarbageCollectionResult> {
        Ok(GarbageCollectionResult::default())
    }
}
