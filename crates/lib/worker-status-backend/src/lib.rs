//! Worker status backend.

use uuid::Uuid;

pub use waymark_backends_core::{BackendError, BackendResult};

/// Worker status update for persistence.
#[derive(Clone, Debug)]
pub struct WorkerStatusUpdate {
    pub pool_id: Uuid,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<chrono::DateTime<chrono::Utc>>,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
    pub dispatch_queue_size: i64,
    pub total_in_flight: i64,
    pub active_workers: i32,
    pub actions_per_sec: f64,
    pub median_instance_duration_secs: Option<f64>,
    pub active_instance_count: i32,
    pub total_instances_completed: i64,
    pub instances_per_sec: f64,
    pub instances_per_min: f64,
    pub time_series: Option<Vec<u8>>,
}

/// Backend capability for recording worker status metrics.
#[async_trait::async_trait]
pub trait WorkerStatusBackend: Send + Sync {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()>;
}
