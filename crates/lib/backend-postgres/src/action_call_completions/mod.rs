//! Postgres backend for durably-stored action-call completions.
//!
//! Implements the `waymark_action_completions_reconciler_backend`
//! traits: completions are recorded as they arrive from the worker pool,
//! polled by demand, deleted on ack, and purged per VM on terminal
//! completion.

pub mod error;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use nonempty_collections::{NESlice, NEVec};
use sqlx::Row;
use waymark_action_completions_reconciler_backend::record_completions;
use waymark_action_completions_reconciler_backend::{CompletionKey, CompletionRecord};
use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::PostgresBackend;

impl waymark_action_completions_reconciler_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

/// Bind-ready column arrays for a batch of completion keys.
struct KeyColumns {
    vm_ids: Vec<InstanceId>,
    promise_state_ids: Vec<i64>,
}

fn key_columns<'a>(
    keys: impl Iterator<Item = &'a CompletionKey<InstanceId>>,
) -> Result<KeyColumns, std::num::TryFromIntError> {
    let mut vm_ids = Vec::new();
    let mut promise_state_ids = Vec::new();
    for key in keys {
        vm_ids.push(key.vm_id);
        promise_state_ids.push(i64::try_from(key.promise_state_id.0)?);
    }
    Ok(KeyColumns {
        vm_ids,
        promise_state_ids,
    })
}

impl waymark_action_completions_reconciler_backend::RecordCompletions for PostgresBackend {
    type Error = error::RecordError;

    #[obs]
    #[function_name::named]
    async fn record_completions(
        &self,
        records: NESlice<'_, CompletionRecord<InstanceId>>,
    ) -> Result<record_completions::RecordingSuccess<InstanceId>, Self::Error> {
        let mut vm_ids = Vec::with_capacity(records.len().get());
        let mut promise_state_ids = Vec::with_capacity(records.len().get());
        let mut effect_numbers = Vec::with_capacity(records.len().get());
        let mut outcomes = Vec::with_capacity(records.len().get());
        for record in records.iter() {
            vm_ids.push(record.vm_id);
            promise_state_ids
                .push(i64::try_from(record.promise_state_id.0).map_err(Self::Error::OutOfRange)?);
            effect_numbers
                .push(i64::try_from(record.effect_number.0).map_err(Self::Error::OutOfRange)?);
            outcomes.push(record.outcome.clone());
        }

        Self::count_query(&self.query_counts, "insert:action_call_completions");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:action_call_completions",
            vm_ids.len(),
        );
        let inserted = sqlx::query(
            r#"
            INSERT INTO action_call_completions
                (vm_id, promise_state_id, effect_number, outcome)
            SELECT * FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[], $4::bytea[])
                AS t(vm_id, promise_state_id, effect_number, outcome)
            ON CONFLICT (vm_id, promise_state_id) DO NOTHING
            RETURNING vm_id, promise_state_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&promise_state_ids)
        .bind(&effect_numbers)
        .bind(&outcomes)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "insert:action_call_completions"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        // Happy path: everything inserted, nothing conflicted.
        if inserted.len() == vm_ids.len() {
            return Ok(record_completions::RecordingSuccess::AllRecorded);
        }

        // Rare path: some keys conflicted with existing rows (or with an
        // earlier row of this same batch).  Identical duplicates are
        // idempotently accepted; a matching effect number with a
        // conflicting outcome is a redelivered non-deterministic retry
        // (first write wins, reported in the success value); a different
        // effect number is a data-integrity violation and fails loudly.
        // A conflicting row acked between the insert and this check simply
        // drops out of the comparison — an acked row means its settlement
        // was already durably applied, so the re-delivery is correctly
        // ignored either way.
        let inserted_keys: HashSet<(InstanceId, i64)> = inserted
            .iter()
            .map(|row| (row.get("vm_id"), row.get("promise_state_id")))
            .collect();

        let mut conflicted_vm_ids = Vec::new();
        let mut conflicted_promise_state_ids = Vec::new();
        let mut conflicted_effect_numbers = Vec::new();
        let mut conflicted_outcomes = Vec::new();
        for (index, vm_id) in vm_ids.iter().enumerate() {
            if inserted_keys.contains(&(*vm_id, promise_state_ids[index])) {
                continue;
            }
            conflicted_vm_ids.push(*vm_id);
            conflicted_promise_state_ids.push(promise_state_ids[index]);
            conflicted_effect_numbers.push(effect_numbers[index]);
            conflicted_outcomes.push(outcomes[index].clone());
        }

        Self::count_query(
            &self.query_counts,
            "select:action_call_completions_divergence",
        );
        let divergent = sqlx::query(
            r#"
            SELECT t.vm_id, t.promise_state_id,
                   (existing.effect_number IS DISTINCT FROM t.effect_number)
                       AS effect_diverges
            FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[], $4::bytea[])
                AS t(vm_id, promise_state_id, effect_number, outcome)
            JOIN action_call_completions existing
                USING (vm_id, promise_state_id)
            WHERE (existing.effect_number, existing.outcome)
                IS DISTINCT FROM (t.effect_number, t.outcome)
            "#,
        )
        .bind(&conflicted_vm_ids)
        .bind(&conflicted_promise_state_ids)
        .bind(&conflicted_effect_numbers)
        .bind(&conflicted_outcomes)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:action_call_completions_divergence"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        let mut divergent_effect_numbers = Vec::new();
        let mut conflicting_outcomes = Vec::new();
        for row in &divergent {
            let promise_state_id: i64 = row.get("promise_state_id");
            let key = CompletionKey {
                vm_id: row.get("vm_id"),
                promise_state_id: PromiseStateId(
                    usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                ),
            };
            if row.get::<bool, _>("effect_diverges") {
                divergent_effect_numbers.push(key);
            } else {
                conflicting_outcomes.push(key);
            }
        }

        if let Some(keys) = NEVec::try_from_vec(divergent_effect_numbers) {
            return Err(Self::Error::DivergentEffectNumber(keys));
        }
        match NEVec::try_from_vec(conflicting_outcomes) {
            Some(keys) => Ok(record_completions::RecordingSuccess::SomeConflictingOutcomes(keys)),
            None => Ok(record_completions::RecordingSuccess::AllRecorded),
        }
    }
}

impl waymark_action_completions_reconciler_backend::PollCompletions for PostgresBackend {
    type Error = error::PollError;

    #[obs]
    #[function_name::named]
    async fn poll_completions(
        &self,
        demand: NESlice<'_, CompletionKey<InstanceId>>,
    ) -> Result<Vec<CompletionRecord<InstanceId>>, Self::Error> {
        let columns = key_columns(demand.iter()).map_err(Self::Error::OutOfRange)?;

        Self::count_query(&self.query_counts, "select:action_call_completions_poll");
        Self::count_batch_size(
            &self.batch_size_counts,
            "select:action_call_completions_poll",
            columns.vm_ids.len(),
        );
        let rows = sqlx::query(
            r#"
            SELECT c.vm_id, c.promise_state_id, c.effect_number, c.outcome
            FROM action_call_completions c
            JOIN UNNEST($1::uuid[], $2::bigint[]) AS d(vm_id, promise_state_id)
                ON c.vm_id = d.vm_id AND c.promise_state_id = d.promise_state_id
            "#,
        )
        .bind(&columns.vm_ids)
        .bind(&columns.promise_state_ids)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:action_call_completions_poll"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        rows.iter()
            .map(|row| {
                let promise_state_id: i64 = row.get("promise_state_id");
                let effect_number: i64 = row.get("effect_number");
                Ok(CompletionRecord {
                    vm_id: row.get("vm_id"),
                    promise_state_id: PromiseStateId(
                        usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                    ),
                    effect_number: EffectNumber(
                        usize::try_from(effect_number).map_err(Self::Error::OutOfRange)?,
                    ),
                    outcome: row.get("outcome"),
                })
            })
            .collect()
    }
}

impl waymark_action_completions_reconciler_backend::AckCompletions for PostgresBackend {
    type Error = error::AckError;

    #[obs]
    #[function_name::named]
    async fn ack_completions(
        &self,
        keys: NESlice<'_, CompletionKey<InstanceId>>,
    ) -> Result<(), Self::Error> {
        let columns = key_columns(keys.iter()).map_err(Self::Error::OutOfRange)?;

        Self::count_query(&self.query_counts, "delete:action_call_completions_ack");
        Self::count_batch_size(
            &self.batch_size_counts,
            "delete:action_call_completions_ack",
            columns.vm_ids.len(),
        );
        sqlx::query(
            r#"
            DELETE FROM action_call_completions c
            USING UNNEST($1::uuid[], $2::bigint[]) AS d(vm_id, promise_state_id)
            WHERE c.vm_id = d.vm_id AND c.promise_state_id = d.promise_state_id
            "#,
        )
        .bind(&columns.vm_ids)
        .bind(&columns.promise_state_ids)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!(
            "delete:action_call_completions_ack"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        Ok(())
    }
}

impl waymark_action_completions_reconciler_backend::PurgeVmCompletions for PostgresBackend {
    type Error = error::PurgeError;

    #[obs]
    #[function_name::named]
    async fn purge_vm_completions(&self, vm_id: &InstanceId) -> Result<(), Self::Error> {
        Self::count_query(&self.query_counts, "delete:action_call_completions_purge");
        sqlx::query("DELETE FROM action_call_completions WHERE vm_id = $1")
            .bind(vm_id)
            .execute(&self.pool)
            .timed(crate::query_timing_histogram!(
                "delete:action_call_completions_purge"
            ))
            .await
            .map_err(Self::Error::Sqlx)?;

        Ok(())
    }
}
