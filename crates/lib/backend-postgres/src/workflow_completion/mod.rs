//! Postgres backend for workflow completion persistence.
//!
//! Implements [`waymark_workflow_completion_backend::RecordOutcomes`] for
//! [`crate::PostgresBackend`].
//!
//! First-write-wins semantics via a single atomic upsert: rows are inserted
//! on first write; on conflict the `WHERE` clause checks that the existing
//! outcome (both the `result` and `error` columns) matches, otherwise the
//! row is not updated and drops out of the `RETURNING` set — the missing
//! keys are reported as conflicts.

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
// RecordOutcomes
// ---------------------------------------------------------------------------

impl waymark_workflow_completion_backend::RecordOutcomes for PostgresBackend {
    type Error = error::RecordOutcomesError;

    #[obs]
    #[function_name::named]
    async fn record_outcomes<'a>(
        &'a self,
        outcomes: nonempty_collections::NESlice<
            'a,
            waymark_workflow_completion_backend::RecordOutcomesItem<'a, InstanceId>,
        >,
    ) -> Result<waymark_workflow_completion_backend::RecordingSuccess<InstanceId>, Self::Error>
    {
        PostgresBackend::count_query(&self.query_counts, "upsert:vm_execution_results_outcome");
        PostgresBackend::count_batch_size(
            &self.batch_size_counts,
            "upsert:vm_execution_results_outcome",
            outcomes.len().get(),
        );

        // One nullable column pair per outcome variant: a completion is
        // `(result, NULL)`, an exception `(NULL, error)` — mirroring the
        // row's own XOR invariant.
        let mut vm_ids = Vec::with_capacity(outcomes.len().get());
        let mut results: Vec<Option<&[u8]>> = Vec::with_capacity(outcomes.len().get());
        let mut errors: Vec<Option<&[u8]>> = Vec::with_capacity(outcomes.len().get());
        for item in outcomes.iter() {
            vm_ids.push(*item.vm_id);
            match item.outcome {
                waymark_workflow_completion_backend::Outcome::Completion(value) => {
                    results.push(Some(value.as_slice()));
                    errors.push(None);
                }
                waymark_workflow_completion_backend::Outcome::Exception(exception) => {
                    results.push(None);
                    errors.push(Some(exception.as_slice()));
                }
            }
        }

        let rows = sqlx::query(
            r#"
            INSERT INTO vm_execution_results (vm_id, result, error)
            SELECT * FROM UNNEST($1::uuid[], $2::bytea[], $3::bytea[])
            ON CONFLICT (vm_id) DO UPDATE
            SET result = EXCLUDED.result, error = EXCLUDED.error
            WHERE vm_execution_results.result IS NOT DISTINCT FROM EXCLUDED.result
                AND vm_execution_results.error IS NOT DISTINCT FROM EXCLUDED.error
            RETURNING vm_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&results)
        .bind(&errors)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "upsert:vm_execution_results_outcome"
        ))
        .await
        .map_err(error::RecordOutcomesError::Sqlx)?;

        // Happy path: every outcome was inserted or identically re-recorded.
        if rows.len() == vm_ids.len() {
            return Ok(waymark_workflow_completion_backend::RecordingSuccess::AllRecorded);
        }

        // Keys missing from `RETURNING` hit a row holding a different
        // outcome — per-row first-write-wins conflicts, reported by name;
        // the rest were durably recorded above.
        let recorded: std::collections::HashSet<InstanceId> =
            rows.iter().map(|row| row.get("vm_id")).collect();
        let conflicted: Vec<InstanceId> = vm_ids
            .iter()
            .copied()
            .filter(|vm_id| !recorded.contains(vm_id))
            .collect();
        Ok(
            waymark_workflow_completion_backend::RecordingSuccess::SomeConflicted(
                nonempty_collections::NEVec::try_from_vec(conflicted)
                    .expect("fewer returned rows than inputs, so at least one key conflicted"),
            ),
        )
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
