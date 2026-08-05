use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use waymark_backends_core::BackendResult;
use waymark_timed_future::TimedFutureExt as _;
use waymark_webapp_core::WorkerStatus;

impl waymark_webapp_backend::WebappBackend for crate::PostgresBackend {
    #[function_name::named]
    async fn worker_status_table_exists(&self) -> bool {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'worker_status'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .timed(crate::query_timing_histogram!(
            "webapp:select:worker_status_table_exists"
        ))
        .await
        .unwrap_or(false)
    }

    #[function_name::named]
    async fn get_worker_statuses(&self, window_minutes: i64) -> BackendResult<Vec<WorkerStatus>> {
        let rows = sqlx::query(
            r#"
            SELECT
                pool_id,
                MAX(active_workers) as active_workers,
                COALESCE(SUM(throughput_per_min), 0) as throughput_per_min,
                COALESCE(SUM(throughput_per_min) / 60.0, 0) as actions_per_sec,
                COALESCE(SUM(total_completed), 0)::BIGINT as total_completed,
                MAX(last_action_at) as last_action_at,
                MAX(updated_at) as updated_at,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_dequeue_ms) as median_dequeue_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_handling_ms) as median_handling_ms,
                MAX(dispatch_queue_size) as dispatch_queue_size,
                MAX(total_in_flight) as total_in_flight,
                MAX(median_instance_duration_secs) as median_instance_duration_secs,
                MAX(active_instance_count) as active_instance_count,
                (
                    SELECT COUNT(*)::BIGINT
                    FROM vm_execution_results ver
                    WHERE ver.result IS NOT NULL
                ) as total_instances_completed,
                MAX(instances_per_sec) as instances_per_sec,
                MAX(instances_per_min) as instances_per_min,
                (
                    SELECT time_series FROM worker_status ws2
                    WHERE ws2.pool_id = worker_status.pool_id
                      AND ws2.time_series IS NOT NULL
                    ORDER BY ws2.updated_at DESC LIMIT 1
                ) as time_series
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            GROUP BY pool_id
            ORDER BY actions_per_sec DESC
            "#,
        )
        .bind(window_minutes)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!("webapp:select:worker_statuses"))
        .await?;

        let mut statuses = Vec::new();
        for row in rows {
            statuses.push(WorkerStatus {
                pool_id: row.get::<Uuid, _>("pool_id"),
                active_workers: row.get::<Option<i32>, _>("active_workers").unwrap_or(0),
                throughput_per_min: row.get::<f64, _>("throughput_per_min"),
                actions_per_sec: row.get::<f64, _>("actions_per_sec"),
                total_completed: row.get::<i64, _>("total_completed"),
                last_action_at: row.get::<Option<DateTime<Utc>>, _>("last_action_at"),
                updated_at: row.get::<DateTime<Utc>, _>("updated_at"),
                median_dequeue_ms: row
                    .get::<Option<f64>, _>("median_dequeue_ms")
                    .map(|v| v as i64),
                median_handling_ms: row
                    .get::<Option<f64>, _>("median_handling_ms")
                    .map(|v| v as i64),
                dispatch_queue_size: row.get::<Option<i64>, _>("dispatch_queue_size"),
                total_in_flight: row.get::<Option<i64>, _>("total_in_flight"),
                median_instance_duration_secs: row
                    .get::<Option<f64>, _>("median_instance_duration_secs"),
                active_instance_count: row
                    .get::<Option<i32>, _>("active_instance_count")
                    .unwrap_or(0),
                total_instances_completed: row
                    .get::<Option<i64>, _>("total_instances_completed")
                    .unwrap_or(0),
                instances_per_sec: row
                    .get::<Option<f64>, _>("instances_per_sec")
                    .unwrap_or(0.0),
                instances_per_min: row
                    .get::<Option<f64>, _>("instances_per_min")
                    .unwrap_or(0.0),
                time_series: row.get::<Option<Vec<u8>>, _>("time_series"),
            });
        }

        Ok(statuses)
    }
}
