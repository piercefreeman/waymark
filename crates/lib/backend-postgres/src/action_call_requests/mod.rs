//! Postgres backend for durably-stored action-call requests.
//!
//! Implements the `waymark_action_effect_reconciler_backend` traits:
//! requests are recorded born-locked at effect emission, locked for
//! delivery at VM revival reconcile, kept alive by lock renewal, unlocked
//! at graceful shutdown.  Rows of a deleted VM are swept by the
//! snapshot-deletion cleanup trigger, so there is no purge operation
//! here.
//! Removal happens in the schema itself: the trigger installed by
//! migration `0017` deletes a request row the moment its completion is
//! recorded.

pub mod error;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use nonempty_collections::{NESlice, NEVec, NonEmptyIterator as _};
use sqlx::Row;
use waymark_action_effect_reconciler_backend::record_action_call_requests;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::{
    RenewalStatus, RequestLockRenewal,
};
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestRecord, RequestLock,
    lock_action_call_requests::VmLockOutcome,
};
use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::PostgresBackend;

impl waymark_action_effect_reconciler_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_action_effect_reconciler_backend::HasLockOwnerId for PostgresBackend {
    type LockOwnerId = uuid::Uuid;
}

impl waymark_action_effect_reconciler_backend::HasTimestamp for PostgresBackend {
    type Timestamp = DateTime<Utc>;
}

/// Bind-ready column arrays for a batch of request keys.
struct KeyColumns {
    vm_ids: Vec<InstanceId>,
    promise_state_ids: Vec<i64>,
}

fn key_columns<'a>(
    keys: impl Iterator<Item = &'a ActionCallRequestKey<InstanceId>>,
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

fn record_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ActionCallRequestRecord<InstanceId>, std::num::TryFromIntError> {
    let promise_state_id: i64 = row.get("promise_state_id");
    let effect_number: i64 = row.get("effect_number");
    Ok(ActionCallRequestRecord {
        vm_id: row.get("vm_id"),
        promise_state_id: PromiseStateId(usize::try_from(promise_state_id)?),
        effect_number: EffectNumber(usize::try_from(effect_number)?),
        request: row.get("request"),
    })
}

impl waymark_action_effect_reconciler_backend::RecordActionCallRequests for PostgresBackend {
    type Error = error::RecordError;

    #[obs]
    #[function_name::named]
    async fn record_action_call_requests(
        &self,
        now: DateTime<Utc>,
        lock: RequestLock<uuid::Uuid, DateTime<Utc>>,
        records: NESlice<'_, ActionCallRequestRecord<InstanceId>>,
    ) -> Result<record_action_call_requests::RecordingSuccess<InstanceId>, Self::Error> {
        let mut vm_ids = Vec::with_capacity(records.len().get());
        let mut promise_state_ids = Vec::with_capacity(records.len().get());
        let mut effect_numbers = Vec::with_capacity(records.len().get());
        let mut requests = Vec::with_capacity(records.len().get());
        for record in records.iter() {
            vm_ids.push(record.vm_id);
            promise_state_ids
                .push(i64::try_from(record.promise_state_id.0).map_err(Self::Error::OutOfRange)?);
            effect_numbers
                .push(i64::try_from(record.effect_number.0).map_err(Self::Error::OutOfRange)?);
            requests.push(record.request.clone());
        }

        Self::count_query(&self.query_counts, "insert:action_call_requests");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:action_call_requests",
            vm_ids.len(),
        );
        let inserted = sqlx::query(
            r#"
            INSERT INTO action_call_requests
                (vm_id, promise_state_id, effect_number, request,
                 locked_by, lock_expires_at)
            SELECT t.*, $5, NOW() + ($6 * interval '1 microsecond')
            FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[], $4::bytea[])
                AS t(vm_id, promise_state_id, effect_number, request)
            -- Canonical lock order: replayed keys contend on existing
            -- rows through the conflict arbiter, so insert in primary
            -- key order like every other multi-row writer on the
            -- trigger-swept tables.
            ORDER BY t.vm_id, t.promise_state_id
            ON CONFLICT (vm_id, promise_state_id) DO NOTHING
            RETURNING vm_id, promise_state_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&promise_state_ids)
        .bind(&effect_numbers)
        .bind(&requests)
        .bind(lock.owner)
        .bind(crate::remaining_micros(now, lock.expires_at))
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "insert:action_call_requests"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        // Happy path: everything inserted born-locked, nothing conflicted.
        if inserted.len() == vm_ids.len() {
            return Ok(record_action_call_requests::RecordingSuccess::AllRecorded);
        }

        // Rare path: some keys conflicted with existing rows (a VM replay
        // re-emitting effects, or an earlier row of this same batch).
        // Identical rows are idempotently accepted — untouched, including
        // their locks — and reported so the caller does not deliver them;
        // a different payload is a data-integrity violation and fails
        // loudly.  A conflicting row removed between the insert and this
        // check (its completion got recorded) simply drops out of the
        // comparison — its outcome is durably known, so not delivering is
        // correct either way.
        let inserted_keys: HashSet<(InstanceId, i64)> = inserted
            .iter()
            .map(|row| (row.get("vm_id"), row.get("promise_state_id")))
            .collect();

        let mut conflicted_vm_ids = Vec::new();
        let mut conflicted_promise_state_ids = Vec::new();
        let mut conflicted_effect_numbers = Vec::new();
        let mut conflicted_requests = Vec::new();
        let mut conflicted_keys = Vec::new();
        for (index, vm_id) in vm_ids.iter().enumerate() {
            if inserted_keys.contains(&(*vm_id, promise_state_ids[index])) {
                continue;
            }
            conflicted_vm_ids.push(*vm_id);
            conflicted_promise_state_ids.push(promise_state_ids[index]);
            conflicted_effect_numbers.push(effect_numbers[index]);
            conflicted_requests.push(requests[index].clone());
            conflicted_keys.push(ActionCallRequestKey {
                vm_id: *vm_id,
                promise_state_id: PromiseStateId(
                    usize::try_from(promise_state_ids[index]).map_err(Self::Error::OutOfRange)?,
                ),
            });
        }

        Self::count_query(&self.query_counts, "select:action_call_requests_divergence");
        let divergent = sqlx::query(
            r#"
            SELECT t.vm_id, t.promise_state_id
            FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[], $4::bytea[])
                AS t(vm_id, promise_state_id, effect_number, request)
            JOIN action_call_requests existing
                USING (vm_id, promise_state_id)
            WHERE (existing.effect_number, existing.request)
                IS DISTINCT FROM (t.effect_number, t.request)
            "#,
        )
        .bind(&conflicted_vm_ids)
        .bind(&conflicted_promise_state_ids)
        .bind(&conflicted_effect_numbers)
        .bind(&conflicted_requests)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:action_call_requests_divergence"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        let mut divergent_keys = Vec::new();
        for row in &divergent {
            let promise_state_id: i64 = row.get("promise_state_id");
            divergent_keys.push(ActionCallRequestKey {
                vm_id: row.get("vm_id"),
                promise_state_id: PromiseStateId(
                    usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                ),
            });
        }

        if let Some(keys) = NEVec::try_from_vec(divergent_keys) {
            return Err(Self::Error::DivergentPayload(keys));
        }
        let keys = NEVec::try_from_vec(conflicted_keys)
            .expect("the happy path returned early, so at least one key conflicted");
        Ok(record_action_call_requests::RecordingSuccess::SomeAlreadyRecorded(keys))
    }
}

impl waymark_action_effect_reconciler_backend::LockActionCallRequests for PostgresBackend {
    type Error = error::LockError;

    #[obs]
    #[function_name::named]
    async fn lock_action_call_requests<'a>(
        &'a self,
        now: DateTime<Utc>,
        lock: RequestLock<uuid::Uuid, DateTime<Utc>>,
        vm_ids: NESlice<'a, InstanceId>,
    ) -> Result<NEVec<VmLockOutcome<InstanceId>>, Self::Error> {
        Self::count_query(&self.query_counts, "update:action_call_requests_lock");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:action_call_requests_lock",
            vm_ids.len().get(),
        );
        let locked_rows = sqlx::query(
            r#"
            -- Canonical lock order: take every target row lock in
            -- primary key order before mutating, so multi-row writers
            -- can never deadlock with each other on overlapping sets.
            -- The FOR UPDATE requalifies the predicate on the current
            -- row version after a lock wait, so a row renewed while we
            -- waited correctly drops out.
            WITH locked AS MATERIALIZED (
                SELECT r.vm_id, r.promise_state_id
                FROM action_call_requests r
                WHERE r.vm_id = ANY($1)
                    AND (r.locked_by IS NULL OR r.lock_expires_at <= NOW())
                ORDER BY r.vm_id, r.promise_state_id
                FOR UPDATE
            )
            UPDATE action_call_requests r
            SET locked_by = $2, lock_expires_at = NOW() + ($3 * interval '1 microsecond')
            FROM locked l
            WHERE r.vm_id = l.vm_id AND r.promise_state_id = l.promise_state_id
            RETURNING r.vm_id, r.promise_state_id, r.effect_number, r.request
            "#,
        )
        .bind(vm_ids.as_ref())
        .bind(lock.owner)
        .bind(crate::remaining_micros(now, lock.expires_at))
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "update:action_call_requests_lock"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        Self::count_query(
            &self.query_counts,
            "select:action_call_requests_held_elsewhere",
        );
        let held_rows = sqlx::query(
            r#"
            SELECT vm_id, promise_state_id
            FROM action_call_requests
            WHERE vm_id = ANY($1) AND locked_by IS DISTINCT FROM $2
            "#,
        )
        .bind(vm_ids.as_ref())
        .bind(lock.owner)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:action_call_requests_held_elsewhere"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        // Group the flat row sets into one outcome per input VM, in input
        // order; VMs with no rows keep their (empty, no-op) outcome.
        let mut outcomes: NEVec<VmLockOutcome<InstanceId>> = vm_ids
            .nonempty_iter()
            .map(|vm_id| VmLockOutcome {
                vm_id: *vm_id,
                locked: Vec::new(),
                held_elsewhere: Vec::new(),
            })
            .collect();
        let index_of: std::collections::HashMap<InstanceId, usize> = vm_ids
            .iter()
            .enumerate()
            .map(|(index, vm_id)| (*vm_id, index))
            .collect();

        for row in &locked_rows {
            let record = record_from_row(row).map_err(Self::Error::OutOfRange)?;
            let index = *index_of
                .get(&record.vm_id)
                .expect("every returned row belongs to an input vm");
            outcomes[index].locked.push(record);
        }

        for row in &held_rows {
            let vm_id: InstanceId = row.get("vm_id");
            let promise_state_id: i64 = row.get("promise_state_id");
            let key = ActionCallRequestKey {
                vm_id,
                promise_state_id: PromiseStateId(
                    usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                ),
            };
            let index = *index_of
                .get(&vm_id)
                .expect("every returned row belongs to an input vm");
            outcomes[index].held_elsewhere.push(key);
        }

        Ok(outcomes)
    }
}

impl waymark_action_effect_reconciler_backend::RenewActionCallRequestLocks for PostgresBackend {
    type Error = error::RenewError;

    #[obs]
    #[function_name::named]
    async fn renew_action_call_request_locks(
        &self,
        now: DateTime<Utc>,
        lock: RequestLock<uuid::Uuid, DateTime<Utc>>,
        keys: NESlice<'_, ActionCallRequestKey<InstanceId>>,
    ) -> Result<NEVec<RequestLockRenewal<InstanceId>>, Self::Error> {
        let columns = key_columns(keys.iter()).map_err(Self::Error::OutOfRange)?;

        Self::count_query(&self.query_counts, "update:action_call_requests_renew");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:action_call_requests_renew",
            columns.vm_ids.len(),
        );
        // One statement: extend the expiry where the lock is still ours,
        // and report every input key's state as of this statement.  The
        // per-key row set (matched via the left join) tells the caller
        // which keys are gone entirely (their completion was recorded, or
        // the VM was purged) and which are held by another owner.
        let rows = sqlx::query(
            r#"
            WITH input(vm_id, promise_state_id) AS (
                SELECT * FROM UNNEST($1::uuid[], $2::bigint[])
            ),
            -- Canonical lock order: take every target row lock in
            -- primary key order before mutating, so multi-row writers
            -- can never deadlock with each other on overlapping sets.
            -- The ownership predicate lives here — a locked row is ours
            -- as long as we hold its lock, so the update needs no
            -- re-check.  FOR UPDATE is stronger than the FOR NO KEY
            -- UPDATE a plain UPDATE takes implicitly; nothing references
            -- these tables by foreign key (0020 rejects them), so
            -- nothing can newly block — revisit the strength if an FK
            -- ever appears.
            locked AS MATERIALIZED (
                SELECT r.vm_id, r.promise_state_id
                FROM action_call_requests r
                JOIN input i
                    ON r.vm_id = i.vm_id AND r.promise_state_id = i.promise_state_id
                WHERE r.locked_by = $3
                ORDER BY r.vm_id, r.promise_state_id
                FOR UPDATE OF r
            ),
            renewed AS (
                UPDATE action_call_requests r
                SET lock_expires_at = NOW() + ($4 * interval '1 microsecond')
                FROM locked l
                WHERE r.vm_id = l.vm_id
                    AND r.promise_state_id = l.promise_state_id
                RETURNING r.vm_id, r.promise_state_id
            )
            SELECT i.vm_id, i.promise_state_id,
                   (n.vm_id IS NOT NULL) AS renewed,
                   (e.vm_id IS NOT NULL) AS present
            FROM input i
            LEFT JOIN renewed n
                ON n.vm_id = i.vm_id AND n.promise_state_id = i.promise_state_id
            LEFT JOIN action_call_requests e
                ON e.vm_id = i.vm_id AND e.promise_state_id = i.promise_state_id
            "#,
        )
        .bind(&columns.vm_ids)
        .bind(&columns.promise_state_ids)
        .bind(lock.owner)
        .bind(crate::remaining_micros(now, lock.expires_at))
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "update:action_call_requests_renew"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        let mut renewals = Vec::with_capacity(rows.len());
        let mut unrenewed_but_present = Vec::new();
        for row in &rows {
            let promise_state_id: i64 = row.get("promise_state_id");
            let key = ActionCallRequestKey {
                vm_id: row.get("vm_id"),
                promise_state_id: PromiseStateId(
                    usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                ),
            };
            if row.get::<bool, _>("renewed") {
                renewals.push(RequestLockRenewal {
                    key,
                    status: RenewalStatus::Renewed,
                });
            } else if row.get::<bool, _>("present") {
                unrenewed_but_present.push(key);
            } else {
                renewals.push(RequestLockRenewal {
                    key,
                    status: RenewalStatus::Missing,
                });
            }
        }

        // `present` above comes from the renewal statement's snapshot,
        // which may predate a concurrent completion's row removal — the
        // update then skips the deleted row while the snapshot still shows
        // it, which must NOT read as another owner taking the lock.
        // Classify these keys from a fresh read instead: gone means the
        // completion won (`Missing`), still ours means the extension went
        // unconfirmed this pass (`Unconfirmed`), and only a different
        // owner on the current row is a real `HeldElsewhere`.
        if let Some(unverified) = NESlice::try_from_slice(&unrenewed_but_present) {
            let columns = key_columns(unverified.iter()).map_err(Self::Error::OutOfRange)?;

            Self::count_query(&self.query_counts, "select:action_call_requests_verify");
            let verify_rows = sqlx::query(
                r#"
                SELECT i.vm_id, i.promise_state_id,
                       (r.vm_id IS NOT NULL) AS present,
                       r.locked_by
                FROM UNNEST($1::uuid[], $2::bigint[]) AS i(vm_id, promise_state_id)
                LEFT JOIN action_call_requests r
                    ON r.vm_id = i.vm_id AND r.promise_state_id = i.promise_state_id
                "#,
            )
            .bind(&columns.vm_ids)
            .bind(&columns.promise_state_ids)
            .fetch_all(&self.pool)
            .timed(crate::query_timing_histogram!(
                "select:action_call_requests_verify"
            ))
            .await
            .map_err(Self::Error::Sqlx)?;

            for row in &verify_rows {
                let promise_state_id: i64 = row.get("promise_state_id");
                let key = ActionCallRequestKey {
                    vm_id: row.get("vm_id"),
                    promise_state_id: PromiseStateId(
                        usize::try_from(promise_state_id).map_err(Self::Error::OutOfRange)?,
                    ),
                };
                let status = if !row.get::<bool, _>("present") {
                    RenewalStatus::Missing
                } else {
                    match row.get::<Option<uuid::Uuid>, _>("locked_by") {
                        Some(locked_by) if locked_by == lock.owner => RenewalStatus::Unconfirmed,
                        // A row we no longer own — relocked by another
                        // owner, or unlocked and up for redelivery by
                        // anyone. Either way our authorization is gone.
                        Some(_) | None => RenewalStatus::HeldElsewhere,
                    }
                };
                renewals.push(RequestLockRenewal { key, status });
            }
        }

        Ok(NEVec::try_from_vec(renewals)
            .expect("the input keys are non-empty and each yields one row"))
    }
}

impl waymark_action_effect_reconciler_backend::UnlockActionCallRequests for PostgresBackend {
    type Error = error::UnlockError;

    #[obs]
    #[function_name::named]
    async fn unlock_action_call_requests(
        &self,
        owner: &uuid::Uuid,
        keys: NESlice<'_, ActionCallRequestKey<InstanceId>>,
    ) -> Result<(), Self::Error> {
        let columns = key_columns(keys.iter()).map_err(Self::Error::OutOfRange)?;

        Self::count_query(&self.query_counts, "update:action_call_requests_unlock");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:action_call_requests_unlock",
            columns.vm_ids.len(),
        );
        sqlx::query(
            r#"
            -- Canonical lock order: see the renewal statement.
            WITH locked AS MATERIALIZED (
                SELECT r.vm_id, r.promise_state_id
                FROM action_call_requests r
                JOIN UNNEST($1::uuid[], $2::bigint[]) AS d(vm_id, promise_state_id)
                    ON r.vm_id = d.vm_id AND r.promise_state_id = d.promise_state_id
                WHERE r.locked_by = $3
                ORDER BY r.vm_id, r.promise_state_id
                FOR UPDATE OF r
            )
            UPDATE action_call_requests r
            SET locked_by = NULL, lock_expires_at = NULL
            FROM locked l
            WHERE r.vm_id = l.vm_id
                AND r.promise_state_id = l.promise_state_id
            "#,
        )
        .bind(&columns.vm_ids)
        .bind(&columns.promise_state_ids)
        .bind(owner)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!(
            "update:action_call_requests_unlock"
        ))
        .await
        .map_err(Self::Error::Sqlx)?;

        Ok(())
    }
}
