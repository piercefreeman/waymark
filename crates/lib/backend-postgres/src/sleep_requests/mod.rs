//! Postgres backend for durably-recorded sleep requests.
//!
//! Implements the `waymark_sleep_reconciler_backend` traits: sleeps are
//! recorded as the VM emits them, polled by demand and dueness, deleted
//! on ack, and purged per VM on terminal completion.

pub mod error;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use nonempty_collections::{NESlice, NEVec};
use sqlx::Row;
use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_sleep_reconciler_backend::{SleepKey, SleepRecord};
use waymark_timed_future::TimedFutureExt as _;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::PostgresBackend;

impl waymark_sleep_reconciler_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_sleep_reconciler_backend::HasTimestamp for PostgresBackend {
    type Timestamp = chrono::DateTime<chrono::Utc>;
}

/// Bind-ready column arrays for a batch of sleep keys.
struct KeyColumns {
    vm_ids: Vec<InstanceId>,
    promise_state_ids: Vec<i64>,
}

fn key_columns<'a>(
    keys: impl Iterator<Item = &'a SleepKey<InstanceId>>,
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

impl waymark_sleep_reconciler_backend::RecordSleeps for PostgresBackend {
    type Error = error::RecordError;

    #[obs]
    #[function_name::named]
    async fn record_sleeps(
        &self,
        records: NESlice<'_, SleepRecord<InstanceId, chrono::DateTime<chrono::Utc>>>,
    ) -> Result<(), Self::Error> {
        let mut vm_ids = Vec::with_capacity(records.len().get());
        let mut promise_state_ids = Vec::with_capacity(records.len().get());
        let mut effect_numbers = Vec::with_capacity(records.len().get());
        let mut wake_ats = Vec::with_capacity(records.len().get());
        for record in records.iter() {
            vm_ids.push(record.vm_id);
            promise_state_ids
                .push(i64::try_from(record.promise_state_id.0).map_err(Self::Error::OutOfRange)?);
            effect_numbers
                .push(i64::try_from(record.effect_number.0).map_err(Self::Error::OutOfRange)?);
            wake_ats.push(record.wake_at);
        }

        Self::count_query(&self.query_counts, "insert:sleep_requests");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:sleep_requests",
            vm_ids.len(),
        );
        let inserted = sqlx::query(
            r#"
            INSERT INTO sleep_requests
                (vm_id, promise_state_id, effect_number, wake_at)
            SELECT * FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[], $4::timestamptz[])
                AS t(vm_id, promise_state_id, effect_number, wake_at)
            ON CONFLICT (vm_id, promise_state_id) DO NOTHING
            RETURNING vm_id, promise_state_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&promise_state_ids)
        .bind(&effect_numbers)
        .bind(&wake_ats)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!("insert:sleep_requests"))
        .await
        .map_err(Self::Error::Sqlx)?;

        // Happy path: everything inserted, nothing conflicted.
        if inserted.len() == vm_ids.len() {
            return Ok(());
        }

        // Rare path: some keys conflicted with existing rows (or with an
        // earlier row of this same batch).  A conflicting row is the
        // expected replay case — the originally recorded wake_at stands,
        // silently — unless its effect number diverges, which is a
        // data-integrity violation and fails loudly.  A conflicting row
        // acked between the insert and this check simply drops out of the
        // comparison — an acked row means its settlement was already
        // durably applied, so the re-delivery is correctly ignored either
        // way.
        let inserted_keys: HashSet<(InstanceId, i64)> = inserted
            .iter()
            .map(|row| (row.get("vm_id"), row.get("promise_state_id")))
            .collect();

        let mut conflicted_vm_ids = Vec::new();
        let mut conflicted_promise_state_ids = Vec::new();
        let mut conflicted_effect_numbers = Vec::new();
        for (index, vm_id) in vm_ids.iter().enumerate() {
            if inserted_keys.contains(&(*vm_id, promise_state_ids[index])) {
                continue;
            }
            conflicted_vm_ids.push(*vm_id);
            conflicted_promise_state_ids.push(promise_state_ids[index]);
            conflicted_effect_numbers.push(effect_numbers[index]);
        }

        Self::count_query(&self.query_counts, "select:sleep_requests_divergence");
        let divergent = sqlx::query(
            r#"
            SELECT t.vm_id, t.promise_state_id
            FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[])
                AS t(vm_id, promise_state_id, effect_number)
            JOIN sleep_requests existing
                USING (vm_id, promise_state_id)
            WHERE existing.effect_number IS DISTINCT FROM t.effect_number
            "#,
        )
        .bind(&conflicted_vm_ids)
        .bind(&conflicted_promise_state_ids)
        .bind(&conflicted_effect_numbers)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:sleep_requests_divergence"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        let mut divergent_keys = Vec::new();
        for row in &divergent {
            let promise_state_id: i64 = row.get("promise_state_id");
            divergent_keys.push(SleepKey {
                vm_id: row.get("vm_id"),
                promise_state_id: PromiseStateId(
                    usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                ),
            });
        }

        match NEVec::try_from_vec(divergent_keys) {
            Some(keys) => Err(Self::Error::DivergentEffectNumber(keys)),
            None => Ok(()),
        }
    }
}

impl waymark_sleep_reconciler_backend::PollDueSleeps for PostgresBackend {
    type Error = error::PollError;

    #[obs]
    #[function_name::named]
    async fn poll_due_sleeps(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        demand: NESlice<'_, SleepKey<InstanceId>>,
    ) -> Result<Vec<SleepKey<InstanceId>>, Self::Error> {
        let columns = key_columns(demand.iter()).map_err(Self::Error::OutOfRange)?;

        Self::count_query(&self.query_counts, "select:sleep_requests_poll");
        Self::count_batch_size(
            &self.batch_size_counts,
            "select:sleep_requests_poll",
            columns.vm_ids.len(),
        );
        let rows = sqlx::query(
            r#"
            SELECT s.vm_id, s.promise_state_id
            FROM sleep_requests s
            JOIN UNNEST($1::uuid[], $2::bigint[]) AS d(vm_id, promise_state_id)
                ON s.vm_id = d.vm_id AND s.promise_state_id = d.promise_state_id
            WHERE s.wake_at <= $3
            "#,
        )
        .bind(&columns.vm_ids)
        .bind(&columns.promise_state_ids)
        .bind(now)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!("select:sleep_requests_poll"))
        .await
        .map_err(Self::Error::Sqlx)?;

        rows.iter()
            .map(|row| {
                let promise_state_id: i64 = row.get("promise_state_id");
                Ok(SleepKey {
                    vm_id: row.get("vm_id"),
                    promise_state_id: PromiseStateId(
                        usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                    ),
                })
            })
            .collect()
    }
}

impl waymark_sleep_reconciler_backend::AckSleeps for PostgresBackend {
    type Error = error::AckError;

    #[obs]
    #[function_name::named]
    async fn ack_sleeps(&self, keys: NESlice<'_, SleepKey<InstanceId>>) -> Result<(), Self::Error> {
        let columns = key_columns(keys.iter()).map_err(Self::Error::OutOfRange)?;

        Self::count_query(&self.query_counts, "delete:sleep_requests_ack");
        Self::count_batch_size(
            &self.batch_size_counts,
            "delete:sleep_requests_ack",
            columns.vm_ids.len(),
        );
        sqlx::query(
            r#"
            DELETE FROM sleep_requests s
            USING UNNEST($1::uuid[], $2::bigint[]) AS d(vm_id, promise_state_id)
            WHERE s.vm_id = d.vm_id AND s.promise_state_id = d.promise_state_id
            "#,
        )
        .bind(&columns.vm_ids)
        .bind(&columns.promise_state_ids)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!("delete:sleep_requests_ack"))
        .await
        .map_err(Self::Error::Sqlx)?;

        Ok(())
    }
}

impl waymark_sleep_reconciler_backend::PurgeVmSleeps for PostgresBackend {
    type Error = error::PurgeError;

    #[obs]
    #[function_name::named]
    async fn purge_vm_sleeps(&self, vm_id: &InstanceId) -> Result<(), Self::Error> {
        Self::count_query(&self.query_counts, "delete:sleep_requests_purge");
        sqlx::query("DELETE FROM sleep_requests WHERE vm_id = $1")
            .bind(vm_id)
            .execute(&self.pool)
            .timed(crate::query_timing_histogram!(
                "delete:sleep_requests_purge"
            ))
            .await
            .map_err(Self::Error::Sqlx)?;

        Ok(())
    }
}
