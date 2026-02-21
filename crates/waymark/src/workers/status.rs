//! Background status reporting helpers for worker pools.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tracing::{info, warn};
use uuid::Uuid;

use crate::backends::{WorkerStatusBackend, WorkerStatusUpdate};
use crate::pool_status::{PoolTimeSeries, TimeSeriesEntry};

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

pub trait WorkerPoolStats: Send + Sync {
    fn stats_snapshot(&self) -> WorkerPoolStatsSnapshot;
}

/// Spawn a background task that reports worker status to the database.
pub fn spawn_status_reporter<B, P>(
    pool_id: Uuid,
    backend: B,
    worker_pool: P,
    active_instances: Arc<AtomicUsize>,
    interval: Duration,
    shutdown: tokio_util::sync::WaitForCancellationFutureOwned,
) -> tokio::task::JoinHandle<()>
where
    B: WorkerStatusBackend + Send + Sync + 'static,
    P: WorkerPoolStats + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut time_series = PoolTimeSeries::new();
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            pool_id = %pool_id,
            interval_ms = interval.as_millis(),
            "status reporter started"
        );

        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let stats = worker_pool.stats_snapshot();
                    let actions_per_sec = stats.throughput_per_min / 60.0;
                    let active_instances_count = active_instances.load(Ordering::SeqCst);
                    let active_instances_u32 =
                        u32::try_from(active_instances_count).unwrap_or(u32::MAX);
                    let active_instances_i32 =
                        i32::try_from(active_instances_count).unwrap_or(i32::MAX);

                    let now = Utc::now();
                    time_series.push(TimeSeriesEntry {
                        timestamp_secs: now.timestamp(),
                        actions_per_sec: actions_per_sec as f32,
                        active_workers: stats.active_workers,
                        median_instance_duration_secs: 0.0,
                        active_instances: active_instances_u32,
                        queue_depth: stats.dispatch_queue_size as u32,
                        in_flight_actions: stats.total_in_flight as u32,
                    });

                    let status = WorkerStatusUpdate {
                        pool_id,
                        throughput_per_min: stats.throughput_per_min,
                        total_completed: stats.total_completed,
                        last_action_at: stats.last_action_at,
                        median_dequeue_ms: stats.median_dequeue_ms,
                        median_handling_ms: stats.median_handling_ms,
                        dispatch_queue_size: stats.dispatch_queue_size as i64,
                        total_in_flight: stats.total_in_flight as i64,
                        active_workers: stats.active_workers as i32,
                        actions_per_sec,
                        median_instance_duration_secs: None,
                        active_instance_count: active_instances_i32,
                        total_instances_completed: 0,
                        instances_per_sec: 0.0,
                        instances_per_min: 0.0,
                        time_series: Some(time_series.encode()),
                    };

                    if let Err(err) = backend.upsert_worker_status(&status).await {
                        warn!(error = %err, "failed to update worker status");
                    }
                }
                _ = &mut shutdown => {
                    info!("status reporter shutting down");
                    break;
                }
            }
        }
    })
}
