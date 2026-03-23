//! Background status reporting helpers for worker pools.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct WorkerPoolStatsSnapshot {
    pub active_workers: u16,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
    pub dispatch_queue_size: usize,
    pub total_in_flight: usize,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
}

pub trait WorkerPoolStats {
    fn stats_snapshot(&self) -> WorkerPoolStatsSnapshot;
}
