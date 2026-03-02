use waymark_worker_status_backend::{BackendResult, WorkerStatusBackend, WorkerStatusUpdate};

#[async_trait::async_trait]
impl WorkerStatusBackend for crate::MemoryBackend {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        let mut stored = self
            .worker_status_updates
            .lock()
            .expect("worker status updates poisoned");
        stored.push(status.clone());
        Ok(())
    }
}
