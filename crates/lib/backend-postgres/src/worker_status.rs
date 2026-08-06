use waymark_backends_core::BackendResult;
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;
use waymark_worker_status_backend::{WorkerStatusBackend, WorkerStatusUpdate};

use super::PostgresBackend;

impl PostgresBackend {
    /// Upsert worker status for monitoring and activity graphs.
    #[obs]
    #[function_name::named]
    pub async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "upsert:worker_status");
        sqlx::query(
            r#"
            INSERT INTO worker_status (
                pool_id,
                worker_id,
                throughput_per_min,
                total_completed,
                last_action_at,
                updated_at,
                median_dequeue_ms,
                median_handling_ms,
                dispatch_queue_size,
                total_in_flight,
                active_workers,
                actions_per_sec,
                median_instance_duration_secs,
                active_instance_count,
                total_instances_completed,
                instances_per_sec,
                instances_per_min,
                time_series
            )
            VALUES ($1, 0, $2, $3, $4, NOW(), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (pool_id, worker_id)
            DO UPDATE SET
                throughput_per_min = EXCLUDED.throughput_per_min,
                total_completed = EXCLUDED.total_completed,
                last_action_at = EXCLUDED.last_action_at,
                updated_at = EXCLUDED.updated_at,
                median_dequeue_ms = EXCLUDED.median_dequeue_ms,
                median_handling_ms = EXCLUDED.median_handling_ms,
                dispatch_queue_size = EXCLUDED.dispatch_queue_size,
                total_in_flight = EXCLUDED.total_in_flight,
                active_workers = EXCLUDED.active_workers,
                actions_per_sec = EXCLUDED.actions_per_sec,
                median_instance_duration_secs = EXCLUDED.median_instance_duration_secs,
                active_instance_count = EXCLUDED.active_instance_count,
                total_instances_completed = EXCLUDED.total_instances_completed,
                instances_per_sec = EXCLUDED.instances_per_sec,
                instances_per_min = EXCLUDED.instances_per_min,
                time_series = EXCLUDED.time_series
            "#,
        )
        .bind(status.pool_id)
        .bind(status.throughput_per_min)
        .bind(status.total_completed)
        .bind(status.last_action_at)
        .bind(status.median_dequeue_ms)
        .bind(status.median_handling_ms)
        .bind(status.dispatch_queue_size)
        .bind(status.total_in_flight)
        .bind(status.active_workers)
        .bind(status.actions_per_sec)
        .bind(status.median_instance_duration_secs)
        .bind(status.active_instance_count)
        .bind(status.total_instances_completed)
        .bind(status.instances_per_sec)
        .bind(status.instances_per_min)
        .bind(&status.time_series)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!("upsert:worker_status"))
        .await?;

        Ok(())
    }
}

impl WorkerStatusBackend for PostgresBackend {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        PostgresBackend::upsert_worker_status(self, status).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serial_test::serial;
    use sqlx::Row;
    use uuid::Uuid;

    use super::super::test_helpers::setup_backend;
    use super::*;

    #[serial(postgres)]
    #[tokio::test]
    async fn worker_status_backend_upsert_worker_status_happy_path() {
        let backend = setup_backend().await;
        let pool_id = Uuid::new_v4();

        WorkerStatusBackend::upsert_worker_status(
            &backend,
            &WorkerStatusUpdate {
                pool_id,
                throughput_per_min: 180.0,
                total_completed: 20,
                last_action_at: Some(Utc::now()),
                median_dequeue_ms: Some(5),
                median_handling_ms: Some(12),
                dispatch_queue_size: 3,
                total_in_flight: 2,
                active_workers: 4,
                actions_per_sec: 3.0,
                median_instance_duration_secs: Some(0.2),
                active_instance_count: 1,
                total_instances_completed: 8,
                instances_per_sec: 0.5,
                instances_per_min: 30.0,
                time_series: None,
            },
        )
        .await
        .expect("upsert worker status");

        let row = sqlx::query(
            "SELECT total_completed, active_workers, actions_per_sec FROM worker_status WHERE pool_id = $1",
        )
        .bind(pool_id)
        .fetch_one(backend.pool())
        .await
        .expect("worker status row");
        assert_eq!(row.get::<i64, _>("total_completed"), 20);
        assert_eq!(row.get::<i32, _>("active_workers"), 4);
        assert_eq!(row.get::<f64, _>("actions_per_sec"), 3.0);
    }
}
