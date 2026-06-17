//! Postgres backend for the workflow service.
//!
//! Implements [`waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime`]
//! for [`crate::PostgresBackend`], including the inherent
//! [`PostgresBackend::register_vm_runtime`] method.

pub mod error;

#[cfg(test)]
mod tests;

use waymark_ids::{InstanceId, WorkflowVersionId};
use waymark_observability::obs;

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
        let mut tx = self.pool.begin().await?;

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
        .await?;

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
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
