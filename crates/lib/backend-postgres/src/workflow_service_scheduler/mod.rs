//! Postgres backend for the workflow-service scheduler surface.
//!
//! Implements the `waymark_workflow_service_scheduler_backend` traits
//! for [`crate::PostgresBackend`].

pub mod error;

#[cfg(test)]
mod tests;

use sqlx::Row as _;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_observability::obs;
use waymark_scheduler_core::ScheduleStatus;
use waymark_timed_future::TimedFutureExt as _;
use waymark_workflow_service_scheduler_backend::{ScheduleRecord, upsert_schedule};

use crate::PostgresBackend;

impl waymark_workflow_service_scheduler_backend::HasExecutableId for PostgresBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_workflow_service_scheduler_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_workflow_service_scheduler_backend::HasTimestamp for PostgresBackend {
    type Timestamp = chrono::DateTime<chrono::Utc>;
}

impl waymark_workflow_service_scheduler_backend::UpsertSchedule for PostgresBackend {
    type Error = error::UpsertScheduleError;

    #[obs]
    #[function_name::named]
    async fn upsert_schedule<'a>(
        &'a self,
        params: upsert_schedule::Params<'a, WorkflowVersionId, chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Self::Error> {
        Self::count_query(&self.query_counts, "upsert:schedules");
        // Re-pointing replaces everything but the last-spawned-instance
        // marker, so overlap suppression still sees a run spawned under
        // the previous registration.
        sqlx::query(
            r#"
            INSERT INTO schedules
                (schedule_name, executable_id, definition, initial_snapshot, status, next_run_at)
            VALUES ($1, $2, $3, $4, 'active', $5)
            ON CONFLICT (schedule_name) DO UPDATE SET
                executable_id = EXCLUDED.executable_id,
                definition = EXCLUDED.definition,
                initial_snapshot = EXCLUDED.initial_snapshot,
                status = EXCLUDED.status,
                next_run_at = EXCLUDED.next_run_at
            "#,
        )
        .bind(params.schedule_name)
        .bind(params.executable_id)
        .bind(params.definition)
        .bind(params.initial_snapshot)
        .bind(params.next_run_at)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!("upsert:schedules"))
        .await
        .map_err(error::UpsertScheduleError::Sqlx)?;
        Ok(())
    }
}

impl waymark_workflow_service_scheduler_backend::GetSchedule for PostgresBackend {
    type Error = error::GetScheduleError;

    #[obs]
    #[function_name::named]
    async fn get_schedule<'a>(
        &'a self,
        schedule_name: &'a str,
    ) -> Result<Option<ScheduleRecord<InstanceId, chrono::DateTime<chrono::Utc>>>, Self::Error>
    {
        Self::count_query(&self.query_counts, "select:schedules_get");
        let row = sqlx::query(&format!(
            "{SCHEDULE_RECORD_SELECT} WHERE schedules.schedule_name = $1"
        ))
        .bind(schedule_name)
        .fetch_optional(&self.pool)
        .timed(crate::query_timing_histogram!("select:schedules_get"))
        .await
        .map_err(error::GetScheduleError::Sqlx)?;

        row.map(|row| {
            schedule_record_from_row(&row).map_err(error::GetScheduleError::InvalidStatus)
        })
        .transpose()
    }
}

impl waymark_workflow_service_scheduler_backend::ListSchedules for PostgresBackend {
    type Error = error::ListSchedulesError;

    #[obs]
    #[function_name::named]
    async fn list_schedules(
        &self,
        status: Option<ScheduleStatus>,
    ) -> Result<Vec<ScheduleRecord<InstanceId, chrono::DateTime<chrono::Utc>>>, Self::Error> {
        Self::count_query(&self.query_counts, "select:schedules_list");
        let rows = sqlx::query(&format!(
            "{SCHEDULE_RECORD_SELECT}
            WHERE $1::text IS NULL OR schedules.status = $1
            ORDER BY schedules.schedule_name"
        ))
        .bind(status.map(|status| status.as_str()))
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!("select:schedules_list"))
        .await
        .map_err(error::ListSchedulesError::Sqlx)?;

        rows.iter()
            .map(|row| {
                schedule_record_from_row(row).map_err(error::ListSchedulesError::InvalidStatus)
            })
            .collect()
    }
}

impl waymark_workflow_service_scheduler_backend::UpdateScheduleStatus for PostgresBackend {
    type Error = error::UpdateScheduleStatusError;

    #[obs]
    #[function_name::named]
    async fn update_schedule_status<'a>(
        &'a self,
        schedule_name: &'a str,
        status: ScheduleStatus,
    ) -> Result<bool, Self::Error> {
        Self::count_query(&self.query_counts, "update:schedules_status");
        let result = sqlx::query(
            r#"
            UPDATE schedules
            SET status = $2
            WHERE schedule_name = $1
            "#,
        )
        .bind(schedule_name)
        .bind(status.as_str())
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!("update:schedules_status"))
        .await
        .map_err(error::UpdateScheduleStatusError::Sqlx)?;
        Ok(result.rows_affected() > 0)
    }
}

impl waymark_workflow_service_scheduler_backend::DeleteSchedule for PostgresBackend {
    type Error = error::DeleteScheduleError;

    #[obs]
    #[function_name::named]
    async fn delete_schedule<'a>(&'a self, schedule_name: &'a str) -> Result<bool, Self::Error> {
        Self::count_query(&self.query_counts, "delete:schedules");
        let result = sqlx::query(
            r#"
            DELETE FROM schedules
            WHERE schedule_name = $1
            "#,
        )
        .bind(schedule_name)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!("delete:schedules"))
        .await
        .map_err(error::DeleteScheduleError::Sqlx)?;
        Ok(result.rows_affected() > 0)
    }
}

/// The shared select list for reading [`ScheduleRecord`] rows; the
/// workflow name is served from the pinned executable, not stored on the
/// schedule row.
const SCHEDULE_RECORD_SELECT: &str = r#"
    SELECT schedules.schedule_name,
           vm_executables.name AS workflow_name,
           schedules.definition,
           schedules.status,
           schedules.next_run_at,
           schedules.last_instance_id
    FROM schedules
    JOIN vm_executables ON vm_executables.id = schedules.executable_id
"#;

fn schedule_record_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<
    ScheduleRecord<InstanceId, chrono::DateTime<chrono::Utc>>,
    waymark_scheduler_core::ParseScheduleStatusError,
> {
    let status: String = row.get("status");
    Ok(ScheduleRecord {
        schedule_name: row.get("schedule_name"),
        workflow_name: row.get("workflow_name"),
        definition: row.get("definition"),
        status: status.parse()?,
        next_run_at: row.get("next_run_at"),
        last_instance_id: row.get("last_instance_id"),
    })
}
