use chrono::{DateTime, Utc};
use sqlx::Row;
use tonic::async_trait;
use uuid::Uuid;

use super::PostgresBackend;
use crate::backends::base::{BackendError, BackendResult, SchedulerBackend};
use crate::scheduler::compute_next_run;
use crate::scheduler::{CreateScheduleParams, ScheduleId, ScheduleType, WorkflowSchedule};

#[async_trait]
impl SchedulerBackend for PostgresBackend {
    async fn upsert_schedule(&self, params: &CreateScheduleParams) -> BackendResult<ScheduleId> {
        let next_run_at = compute_next_run(
            params.schedule_type,
            params.cron_expression.as_deref(),
            params.interval_seconds,
            params.jitter_seconds,
            None,
        )
        .map_err(BackendError::Message)?;

        let row = sqlx::query(
            r#"
            INSERT INTO workflow_schedules
                (workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                 jitter_seconds, input_payload, next_run_at, priority, allow_duplicate)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (workflow_name, schedule_name)
            DO UPDATE SET
                schedule_type = EXCLUDED.schedule_type,
                cron_expression = EXCLUDED.cron_expression,
                interval_seconds = EXCLUDED.interval_seconds,
                jitter_seconds = EXCLUDED.jitter_seconds,
                input_payload = EXCLUDED.input_payload,
                next_run_at = COALESCE(workflow_schedules.next_run_at, EXCLUDED.next_run_at),
                priority = EXCLUDED.priority,
                allow_duplicate = EXCLUDED.allow_duplicate,
                status = 'active',
                updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(&params.workflow_name)
        .bind(&params.schedule_name)
        .bind(params.schedule_type.as_str())
        .bind(&params.cron_expression)
        .bind(params.interval_seconds)
        .bind(params.jitter_seconds)
        .bind(&params.input_payload)
        .bind(next_run_at)
        .bind(params.priority)
        .bind(params.allow_duplicate)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        Ok(ScheduleId(id))
    }

    async fn get_schedule(&self, id: ScheduleId) -> BackendResult<WorkflowSchedule> {
        let schedule = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BackendError::Message(format!("schedule not found: {}", id)))?;

        Ok(schedule.into())
    }

    async fn get_schedule_by_name(
        &self,
        workflow_name: &str,
        schedule_name: &str,
    ) -> BackendResult<Option<WorkflowSchedule>> {
        let schedule = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE workflow_name = $1 AND schedule_name = $2 AND status != 'deleted'
            "#,
        )
        .bind(workflow_name)
        .bind(schedule_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(schedule.map(Into::into))
    }

    async fn list_schedules(
        &self,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<WorkflowSchedule>> {
        let rows = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY workflow_name, schedule_name
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn count_schedules(&self) -> BackendResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_schedules WHERE status != 'deleted'",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    async fn update_schedule_status(&self, id: ScheduleId, status: &str) -> BackendResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_schedule(&self, id: ScheduleId) -> BackendResult<bool> {
        SchedulerBackend::update_schedule_status(self, id, "deleted").await
    }

    async fn find_due_schedules(&self, limit: i32) -> BackendResult<Vec<WorkflowSchedule>> {
        let rows = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE status = 'active'
              AND next_run_at IS NOT NULL
              AND next_run_at <= NOW()
            ORDER BY next_run_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn has_running_instance(&self, _schedule_id: ScheduleId) -> BackendResult<bool> {
        Ok(false)
    }

    async fn mark_schedule_executed(
        &self,
        schedule_id: ScheduleId,
        instance_id: Uuid,
    ) -> BackendResult<()> {
        let schedule = SchedulerBackend::get_schedule(self, schedule_id).await?;
        let schedule_type = ScheduleType::parse(&schedule.schedule_type)
            .ok_or_else(|| BackendError::Message("invalid schedule type".to_string()))?;
        let next_run_at = compute_next_run(
            schedule_type,
            schedule.cron_expression.as_deref(),
            schedule.interval_seconds,
            schedule.jitter_seconds,
            Some(Utc::now()),
        )
        .map_err(BackendError::Message)?;

        sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET last_run_at = NOW(),
                last_instance_id = $2,
                next_run_at = $3,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id.0)
        .bind(instance_id)
        .bind(next_run_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn skip_schedule_run(&self, schedule_id: ScheduleId) -> BackendResult<()> {
        let schedule = SchedulerBackend::get_schedule(self, schedule_id).await?;
        let schedule_type = ScheduleType::parse(&schedule.schedule_type)
            .ok_or_else(|| BackendError::Message("invalid schedule type".to_string()))?;
        let next_run_at = compute_next_run(
            schedule_type,
            schedule.cron_expression.as_deref(),
            schedule.interval_seconds,
            schedule.jitter_seconds,
            Some(Utc::now()),
        )
        .map_err(BackendError::Message)?;

        sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET next_run_at = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id.0)
        .bind(next_run_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct ScheduleRow {
    id: Uuid,
    workflow_name: String,
    schedule_name: String,
    schedule_type: String,
    cron_expression: Option<String>,
    interval_seconds: Option<i64>,
    jitter_seconds: i64,
    input_payload: Option<Vec<u8>>,
    status: String,
    next_run_at: Option<DateTime<Utc>>,
    last_run_at: Option<DateTime<Utc>>,
    last_instance_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    priority: i32,
    allow_duplicate: bool,
}

impl From<ScheduleRow> for WorkflowSchedule {
    fn from(row: ScheduleRow) -> Self {
        Self {
            id: row.id,
            workflow_name: row.workflow_name,
            schedule_name: row.schedule_name,
            schedule_type: row.schedule_type,
            cron_expression: row.cron_expression,
            interval_seconds: row.interval_seconds,
            jitter_seconds: row.jitter_seconds,
            input_payload: row.input_payload,
            status: row.status,
            next_run_at: row.next_run_at,
            last_run_at: row.last_run_at,
            last_instance_id: row.last_instance_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            priority: row.priority,
            allow_duplicate: row.allow_duplicate,
        }
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
    use crate::backends::SchedulerBackend;
    use crate::scheduler::CreateScheduleParams;

    fn sample_params(schedule_name: &str) -> CreateScheduleParams {
        CreateScheduleParams {
            workflow_name: "tests.workflow".to_string(),
            schedule_name: schedule_name.to_string(),
            schedule_type: ScheduleType::Interval,
            cron_expression: None,
            interval_seconds: Some(60),
            jitter_seconds: 0,
            input_payload: Some(vec![1, 2, 3]),
            priority: 3,
            allow_duplicate: true,
        }
    }

    async fn insert_schedule(backend: &PostgresBackend, schedule_name: &str) -> ScheduleId {
        SchedulerBackend::upsert_schedule(backend, &sample_params(schedule_name))
            .await
            .expect("upsert schedule")
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_upsert_schedule_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "upsert").await;
        let row = sqlx::query("SELECT id FROM workflow_schedules WHERE id = $1")
            .bind(id.0)
            .fetch_one(backend.pool())
            .await
            .expect("select schedule");

        assert_eq!(row.get::<Uuid, _>("id"), id.0);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_upsert_schedule_preserves_existing_next_run_at() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "preserve-next-run").await;
        sqlx::query(
            "UPDATE workflow_schedules SET next_run_at = NOW() + INTERVAL '2 days' WHERE id = $1",
        )
        .bind(id.0)
        .execute(backend.pool())
        .await
        .expect("force next_run_at");

        let before: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT next_run_at FROM workflow_schedules WHERE id = $1")
                .bind(id.0)
                .fetch_one(backend.pool())
                .await
                .expect("select next_run_at before");

        let upserted_id =
            SchedulerBackend::upsert_schedule(&backend, &sample_params("preserve-next-run"))
                .await
                .expect("upsert existing schedule");
        assert_eq!(upserted_id.0, id.0);

        let after: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT next_run_at FROM workflow_schedules WHERE id = $1")
                .bind(id.0)
                .fetch_one(backend.pool())
                .await
                .expect("select next_run_at after");

        assert_eq!(after, before);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_get_schedule_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "get").await;
        let schedule = SchedulerBackend::get_schedule(&backend, id)
            .await
            .expect("get schedule");

        assert_eq!(schedule.id, id.0);
        assert_eq!(schedule.schedule_name, "get");
        assert_eq!(schedule.workflow_name, "tests.workflow");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_get_schedule_by_name_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "by-name").await;
        let schedule =
            SchedulerBackend::get_schedule_by_name(&backend, "tests.workflow", "by-name")
                .await
                .expect("get schedule by name")
                .expect("expected schedule");

        assert_eq!(schedule.id, id.0);
        assert_eq!(schedule.schedule_name, "by-name");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_list_schedules_happy_path() {
        let backend = setup_backend().await;

        insert_schedule(&backend, "a-list").await;
        insert_schedule(&backend, "b-list").await;

        let schedules = SchedulerBackend::list_schedules(&backend, 10, 0)
            .await
            .expect("list schedules");

        assert_eq!(schedules.len(), 2);
        assert_eq!(schedules[0].schedule_name, "a-list");
        assert_eq!(schedules[1].schedule_name, "b-list");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_count_schedules_happy_path() {
        let backend = setup_backend().await;

        insert_schedule(&backend, "count-a").await;
        insert_schedule(&backend, "count-b").await;

        let count = SchedulerBackend::count_schedules(&backend)
            .await
            .expect("count schedules");
        assert_eq!(count, 2);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_update_schedule_status_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "status").await;
        let updated = SchedulerBackend::update_schedule_status(&backend, id, "paused")
            .await
            .expect("update schedule status");
        assert!(updated);

        let status: String =
            sqlx::query_scalar("SELECT status FROM workflow_schedules WHERE id = $1")
                .bind(id.0)
                .fetch_one(backend.pool())
                .await
                .expect("select status");
        assert_eq!(status, "paused");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_delete_schedule_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "delete").await;
        let deleted = SchedulerBackend::delete_schedule(&backend, id)
            .await
            .expect("delete schedule");
        assert!(deleted);

        let status: String =
            sqlx::query_scalar("SELECT status FROM workflow_schedules WHERE id = $1")
                .bind(id.0)
                .fetch_one(backend.pool())
                .await
                .expect("select status");
        assert_eq!(status, "deleted");
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_find_due_schedules_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "due").await;
        sqlx::query(
            "UPDATE workflow_schedules SET next_run_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
        )
        .bind(id.0)
        .execute(backend.pool())
        .await
        .expect("force schedule due");

        let due = SchedulerBackend::find_due_schedules(&backend, 10)
            .await
            .expect("find due schedules");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, id.0);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_has_running_instance_happy_path() {
        let backend = setup_backend().await;

        let has_running = SchedulerBackend::has_running_instance(&backend, ScheduleId::new())
            .await
            .expect("has running instance");
        assert!(!has_running);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_mark_schedule_executed_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "mark-executed").await;
        let instance_id = Uuid::new_v4();
        SchedulerBackend::mark_schedule_executed(&backend, id, instance_id)
            .await
            .expect("mark schedule executed");

        let row = sqlx::query(
            "SELECT last_instance_id, last_run_at, next_run_at FROM workflow_schedules WHERE id = $1",
        )
        .bind(id.0)
        .fetch_one(backend.pool())
        .await
        .expect("select schedule");

        let last_instance_id: Option<Uuid> = row.get("last_instance_id");
        let last_run_at: Option<chrono::DateTime<Utc>> = row.get("last_run_at");
        let next_run_at: Option<chrono::DateTime<Utc>> = row.get("next_run_at");

        assert_eq!(last_instance_id, Some(instance_id));
        assert!(last_run_at.is_some());
        assert!(next_run_at.is_some());
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn scheduler_skip_schedule_run_happy_path() {
        let backend = setup_backend().await;

        let id = insert_schedule(&backend, "skip").await;
        sqlx::query(
            "UPDATE workflow_schedules SET next_run_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
        )
        .bind(id.0)
        .execute(backend.pool())
        .await
        .expect("force schedule due");

        SchedulerBackend::skip_schedule_run(&backend, id)
            .await
            .expect("skip schedule run");

        let next_run_at: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT next_run_at FROM workflow_schedules WHERE id = $1")
                .bind(id.0)
                .fetch_one(backend.pool())
                .await
                .expect("select next_run_at");
        assert!(next_run_at.expect("next_run_at").gt(&Utc::now()));
    }
}
