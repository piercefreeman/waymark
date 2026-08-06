//! Shared types for the webapp.

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: &'static str,
}

/// Full worker status for webapp display.
#[derive(Debug, Clone)]
pub struct WorkerStatus {
    pub pool_id: Uuid,
    pub active_workers: i32,
    pub throughput_per_min: f64,
    pub actions_per_sec: f64,
    pub total_completed: i64,
    pub last_action_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
    pub dispatch_queue_size: Option<i64>,
    pub total_in_flight: Option<i64>,
    pub median_instance_duration_secs: Option<f64>,
    pub active_instance_count: i32,
    pub total_instances_completed: i64,
    pub instances_per_sec: f64,
    pub instances_per_min: f64,
    pub time_series: Option<Vec<u8>>,
}
