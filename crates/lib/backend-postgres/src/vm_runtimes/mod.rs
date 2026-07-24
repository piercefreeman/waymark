//! Postgres backend for VM runtime snapshot persistence.

pub mod error;

#[cfg(test)]
mod tests;

use sqlx::Row;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;

use crate::PostgresBackend;

impl waymark_state_vm_runtimes_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_state_vm_runtimes_backend::HasExecutableId for PostgresBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_state_vm_runtimes_backend::StoreSnapshots for PostgresBackend {
    type Error = error::StoreSnapshotsError;

    #[obs]
    #[function_name::named]
    async fn store_snapshots<'a>(
        &'a self,
        snapshots: &'a [waymark_state_vm_runtimes_backend::StoreSnapshotsItem<'a, InstanceId>],
    ) -> Result<(), Self::Error> {
        Self::count_query(&self.query_counts, "update:vm_runtime_snapshots_snapshot");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:vm_runtime_snapshots_snapshot",
            snapshots.len(),
        );

        let mut vm_ids = Vec::with_capacity(snapshots.len());
        let mut blobs: Vec<&[u8]> = Vec::with_capacity(snapshots.len());
        for item in snapshots {
            vm_ids.push(*item.vm_id);
            blobs.push(item.snapshot);
        }

        // Missing rows (VM already completed/deleted) simply match nothing —
        // a benign no-op, so `rows_affected` is not inspected.
        sqlx::query(
            r#"
            UPDATE vm_runtime_snapshots AS s
            SET snapshot = b.snapshot, updated_at = NOW()
            FROM UNNEST($1::uuid[], $2::bytea[]) AS b(vm_id, snapshot)
            WHERE s.vm_id = b.vm_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&blobs)
        .execute(&self.pool)
        .timed(crate::query_timing_histogram!(
            "update:vm_runtime_snapshots_snapshot"
        ))
        .await
        .map_err(error::StoreSnapshotsError::Sqlx)?;

        Ok(())
    }
}

impl waymark_state_vm_runtimes_backend::LoadForRevive for PostgresBackend {
    type Error = error::LoadForReviveError;

    #[obs]
    #[function_name::named]
    async fn load_for_revive<'a>(
        &'a self,
        vm_id: &'a InstanceId,
    ) -> Result<waymark_state_vm_runtimes_backend::RevivePayload<WorkflowVersionId>, Self::Error>
    {
        Self::count_query(&self.query_counts, "select:vm_runtime_snapshots_for_revive");
        let row = sqlx::query(
            r#"
            SELECT snapshot, executable_id
            FROM vm_runtime_snapshots
            WHERE vm_id = $1
            "#,
        )
        .bind(vm_id)
        .fetch_optional(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:vm_runtime_snapshots_for_revive"
        ))
        .await
        .map_err(error::LoadForReviveError::Sqlx)?
        .ok_or(error::LoadForReviveError::NotFound(*vm_id))?;

        let snapshot: Vec<u8> = row.get("snapshot");
        let executable_id: WorkflowVersionId = row.get("executable_id");

        Ok(waymark_state_vm_runtimes_backend::RevivePayload {
            snapshot,
            executable_id,
        })
    }
}
