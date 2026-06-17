//! Postgres backend for the workflow service.
//!
//! Implements [`waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime`]
//! and [`waymark_workflow_service_vm_runtimes_backend::FindExistingVmRuntimes`]
//! for [`crate::PostgresBackend`].

pub mod error;

#[cfg(test)]
mod tests;

use nonempty_collections::NESlice;
use sqlx::Row as _;
use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;

use crate::PostgresBackend;

impl waymark_workflow_service_vm_runtimes_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_workflow_service_vm_runtimes_backend::HasExecutableId for PostgresBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime for PostgresBackend {
    type Error = error::RegisterVmRuntimeError;

    #[obs]
    #[function_name::named]
    async fn register_vm_runtime(
        &self,
        vm_id: &Self::VmId,
        executable_id: &Self::ExecutableId,
        snapshot: &[u8],
    ) -> Result<(), Self::Error> {
        Self::count_query(&self.query_counts, "insert:vm_runtime_snapshots_register");
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(error::RegisterVmRuntimeError::Sqlx)?;

        let result = sqlx::query(
            r#"
            INSERT INTO vm_runtime_snapshots (vm_id, executable_id, snapshot)
            VALUES ($1, $2, $3)
            ON CONFLICT (vm_id) DO NOTHING
            "#,
        )
        .bind(vm_id)
        .bind(executable_id)
        .bind(snapshot)
        .execute(&mut *tx)
        .timed(crate::query_timing_histogram!(
            "insert:vm_runtime_snapshots_register"
        ))
        .await
        .map_err(error::RegisterVmRuntimeError::Sqlx)?;

        if result.rows_affected() == 0 {
            return Err(error::RegisterVmRuntimeError::AlreadyExists(*vm_id));
        }

        sqlx::query(
            r#"
            INSERT INTO workload_pinnings (instance_id)
            VALUES ($1)
            "#,
        )
        .bind(vm_id)
        .execute(&mut *tx)
        .await
        .map_err(error::RegisterVmRuntimeError::Sqlx)?;

        tx.commit()
            .await
            .map_err(error::RegisterVmRuntimeError::Sqlx)?;
        Ok(())
    }
}

impl waymark_workflow_service_vm_runtimes_backend::FindExistingVmRuntimes for PostgresBackend {
    type Error = error::FindExistingVmRuntimesError;

    #[obs]
    #[function_name::named]
    async fn find_existing_vm_runtimes<'a>(
        &'a self,
        vm_ids: NESlice<'a, Self::VmId>,
    ) -> Result<Vec<Self::VmId>, Self::Error> {
        Self::count_query(&self.query_counts, "select:vm_runtime_snapshots_existing");
        let rows = sqlx::query(
            r#"
            SELECT vm_id
            FROM vm_runtime_snapshots
            WHERE vm_id = ANY($1)
            "#,
        )
        .bind(vm_ids.as_ref())
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "select:vm_runtime_snapshots_existing"
        ))
        .await
        .map_err(error::FindExistingVmRuntimesError::Sqlx)?;

        Ok(rows.iter().map(|row| row.get("vm_id")).collect())
    }
}
