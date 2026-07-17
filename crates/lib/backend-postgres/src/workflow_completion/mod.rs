//! Postgres backend for workflow completion persistence.
//!
//! Implements [`waymark_workflow_completion_backend::RecordCompletion`] and
//! [`waymark_workflow_completion_backend::RecordException`] for
//! [`crate::PostgresBackend`].
//!
//! First-write-wins semantics via a single atomic upsert: the row is inserted
//! on first write; on conflict the `WHERE` clause checks that the existing
//! value matches, otherwise no row is updated and the caller detects a
//! conflict via the empty `RETURNING` result.

pub mod error;

#[cfg(test)]
mod tests;

use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;

use sqlx::Row as _;

use crate::PostgresBackend;

// ---------------------------------------------------------------------------
// HasVmId
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

// ---------------------------------------------------------------------------
// RecordCompletion
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::RecordCompletion for PostgresBackend {
    type Error = error::RecordError;

    #[obs]
    #[function_name::named]
    async fn record_completion<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        value: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        let value = value.as_ref();
        PostgresBackend::count_query(&self.query_counts, "upsert:vm_execution_results_completion");
        let returned: Option<i32> = sqlx::query_scalar(
            r#"
            INSERT INTO vm_execution_results (vm_id, result)
            VALUES ($1, $2)
            ON CONFLICT (vm_id) DO UPDATE
            SET result = EXCLUDED.result
            WHERE vm_execution_results.result IS NOT DISTINCT FROM EXCLUDED.result
            RETURNING 1
            "#,
        )
        .bind(vm_id)
        .bind(value)
        .fetch_optional(&self.pool)
        .timed(crate::query_timing_histogram!(
            "upsert:vm_execution_results_completion"
        ))
        .await
        .map_err(error::RecordError::Sqlx)?;

        match returned {
            Some(_) => Ok(()),
            None => Err(error::RecordError::Conflict(*vm_id)),
        }
    }
}

// ---------------------------------------------------------------------------
// RecordException
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::RecordException for PostgresBackend {
    type Error = error::RecordError;

    #[obs]
    #[function_name::named]
    async fn record_exception<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
        exception: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        let exception = exception.as_ref();
        PostgresBackend::count_query(&self.query_counts, "upsert:vm_execution_results_exception");
        let returned: Option<i32> = sqlx::query_scalar(
            r#"
            INSERT INTO vm_execution_results (vm_id, error)
            VALUES ($1, $2)
            ON CONFLICT (vm_id) DO UPDATE
            SET error = EXCLUDED.error
            WHERE vm_execution_results.error IS NOT DISTINCT FROM EXCLUDED.error
            RETURNING 1
            "#,
        )
        .bind(vm_id)
        .bind(exception)
        .fetch_optional(&self.pool)
        .timed(crate::query_timing_histogram!(
            "upsert:vm_execution_results_exception"
        ))
        .await
        .map_err(error::RecordError::Sqlx)?;

        match returned {
            Some(_) => Ok(()),
            None => Err(error::RecordError::Conflict(*vm_id)),
        }
    }
}

impl waymark_workflow_completion_backend::PollOutcome for PostgresBackend {
    type Error = error::PollError;

    #[obs]
    #[function_name::named]
    async fn poll_outcome<'a>(
        &'a self,
        vm_id: &'a Self::VmId,
    ) -> Result<Option<waymark_workflow_completion_backend::Outcome>, Self::Error> {
        use waymark_workflow_completion_backend::Outcome;

        PostgresBackend::count_query(&self.query_counts, "poll:vm_execution_results");
        let row = sqlx::query(
            r#"
            SELECT result, error
            FROM vm_execution_results
            WHERE vm_id = $1
            "#,
        )
        .bind(vm_id)
        .fetch_optional(&self.pool)
        .timed(crate::query_timing_histogram!("poll:vm_execution_results"))
        .await
        .map_err(error::PollError::Sqlx)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let result_bytes: Option<Vec<u8>> = row.get("result");
        let error_bytes: Option<Vec<u8>> = row.get("error");

        match (result_bytes, error_bytes) {
            (Some(completion), None) => Ok(Some(Outcome::Completion(completion))),
            (None, Some(exception)) => Ok(Some(Outcome::Exception(exception))),
            _ => Err(error::PollError::CorruptRow(*vm_id)),
        }
    }
}
