//! Postgres backend for pending action call persistence.
//!
//! Implements the [`waymark_action_reconciler_backend`] traits for
//! [`crate::PostgresBackend`].
//!
//! Stores are idempotent for identical values via a single atomic upsert:
//! the row is inserted on first write; on conflict the `WHERE` clause checks
//! that the existing row matches, otherwise no row is updated and the caller
//! detects a divergence via the empty `RETURNING` result.

pub mod error;

#[cfg(test)]
mod tests;

use sqlx::Row as _;
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::PostgresBackend;

impl waymark_action_reconciler_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_action_reconciler_backend::StorePendingActionCall for PostgresBackend {
    type Error = error::StoreError;

    #[obs]
    #[function_name::named]
    async fn store_pending_action_call<'a>(
        &'a self,
        vm_id: &'a InstanceId,
        correlation: ActionCallCorrelation,
        payload: impl AsRef<[u8]> + Send + 'a,
    ) -> Result<(), Self::Error> {
        let payload = payload.as_ref();
        let promise_state_id = i64::try_from(correlation.promise_state_id.0).map_err(|_| {
            error::StoreError::PromiseStateIdOutOfRange(correlation.promise_state_id)
        })?;
        let effect_number = i64::try_from(correlation.effect_number.0)
            .map_err(|_| error::StoreError::EffectNumberOutOfRange(correlation.effect_number))?;

        Self::count_query(&self.query_counts, "upsert:pending_action_calls");
        let returned: Option<i32> = sqlx::query_scalar(
            r#"
            INSERT INTO pending_action_calls (vm_id, promise_state_id, effect_number, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (vm_id, promise_state_id) DO UPDATE
            SET effect_number = EXCLUDED.effect_number, payload = EXCLUDED.payload
            WHERE pending_action_calls.effect_number IS NOT DISTINCT FROM EXCLUDED.effect_number
              AND pending_action_calls.payload IS NOT DISTINCT FROM EXCLUDED.payload
            RETURNING 1
            "#,
        )
        .bind(vm_id)
        .bind(promise_state_id)
        .bind(effect_number)
        .bind(payload)
        .fetch_optional(&self.pool)
        .timed(crate::query_timing_histogram!(
            "upsert:pending_action_calls"
        ))
        .await
        .map_err(error::StoreError::Sqlx)?;

        match returned {
            Some(_) => Ok(()),
            None => Err(error::StoreError::Conflict {
                vm_id: *vm_id,
                promise_state_id: correlation.promise_state_id,
            }),
        }
    }
}

impl waymark_action_reconciler_backend::RemovePendingActionCall for PostgresBackend {
    type Error = error::RemoveError;

    #[obs]
    #[function_name::named]
    async fn remove_pending_action_call<'a>(
        &'a self,
        vm_id: &'a InstanceId,
        promise_state_id: PromiseStateId,
    ) -> Result<(), Self::Error> {
        let promise_state_id_column = i64::try_from(promise_state_id.0)
            .map_err(|_| error::RemoveError::PromiseStateIdOutOfRange(promise_state_id))?;

        Self::count_query(&self.query_counts, "delete:pending_action_calls");
        sqlx::query(
            r#"
            DELETE FROM pending_action_calls
            WHERE vm_id = $1 AND promise_state_id = $2
            "#,
        )
        .bind(vm_id)
        .bind(promise_state_id_column)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!(
            "delete:pending_action_calls"
        ))
        .await
        .map_err(error::RemoveError::Sqlx)?;

        // Removing an absent record is not an error — removal races
        // benignly with reconciliation cleanup.
        Ok(())
    }
}

impl waymark_action_reconciler_backend::LoadPendingActionCalls for PostgresBackend {
    type Error = error::LoadError;

    #[obs]
    #[function_name::named]
    async fn load_pending_action_calls<'a>(
        &'a self,
        vm_id: &'a InstanceId,
    ) -> Result<Vec<waymark_action_reconciler_backend::PendingActionCall>, Self::Error> {
        Self::count_query(&self.query_counts, "select:pending_action_calls");
        let rows = sqlx::query(
            r#"
            SELECT promise_state_id, effect_number, payload
            FROM pending_action_calls
            WHERE vm_id = $1
            ORDER BY effect_number
            "#,
        )
        .bind(vm_id)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:pending_action_calls"
        ))
        .await
        .map_err(error::LoadError::Sqlx)?;

        rows.into_iter()
            .map(|row| {
                let promise_state_id: i64 = row.get("promise_state_id");
                let effect_number: i64 = row.get("effect_number");
                let payload: Vec<u8> = row.get("payload");

                let promise_state_id = usize::try_from(promise_state_id)
                    .map_err(|_| error::LoadError::PromiseStateIdOutOfRange(promise_state_id))?;
                let effect_number = usize::try_from(effect_number)
                    .map_err(|_| error::LoadError::EffectNumberOutOfRange(effect_number))?;

                Ok(waymark_action_reconciler_backend::PendingActionCall {
                    correlation: ActionCallCorrelation {
                        effect_number: EffectNumber(effect_number),
                        promise_state_id: PromiseStateId(promise_state_id),
                    },
                    payload,
                })
            })
            .collect()
    }
}
