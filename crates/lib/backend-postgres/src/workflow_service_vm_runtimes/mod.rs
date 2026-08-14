//! Postgres backend for the workflow service.
//!
//! Implements [`waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntimes`]
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

impl waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntimes for PostgresBackend {
    type Error = error::RegisterVmRuntimesError;

    #[obs]
    #[function_name::named]
    async fn register_vm_runtimes<'a>(
        &'a self,
        runtimes: nonempty_collections::NESlice<
            'a,
            waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegisterVmRuntimesItem<
                'a,
                InstanceId,
                WorkflowVersionId,
            >,
        >,
    ) -> Result<
        waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess<
            InstanceId,
        >,
        Self::Error,
    > {
        Self::count_query(&self.query_counts, "insert:vm_runtime_snapshots_register");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:vm_runtime_snapshots_register",
            runtimes.len().get(),
        );

        let mut vm_ids = Vec::with_capacity(runtimes.len().get());
        let mut executable_ids = Vec::with_capacity(runtimes.len().get());
        let mut snapshots: Vec<&[u8]> = Vec::with_capacity(runtimes.len().get());
        for item in runtimes.iter() {
            vm_ids.push(*item.vm_id);
            executable_ids.push(*item.executable_id);
            snapshots.push(item.snapshot);
        }

        // One statement, one implicit transaction: only freshly inserted
        // snapshots get a workload row (the CTE feeds the second insert),
        // so an already-registered VM runtime is left fully untouched.
        let rows = sqlx::query(
            r#"
            WITH inserted AS (
                INSERT INTO vm_runtime_snapshots (vm_id, executable_id, snapshot)
                SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::bytea[])
                ON CONFLICT (vm_id) DO NOTHING
                RETURNING vm_id
            )
            INSERT INTO runnable_workloads (workload_id)
            SELECT vm_id FROM inserted
            RETURNING workload_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&executable_ids)
        .bind(&snapshots)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "insert:vm_runtime_snapshots_register"
        ))
        .await
        .map_err(error::RegisterVmRuntimesError::Sqlx)?;

        // Happy path: every VM runtime was freshly registered.
        if rows.len() == vm_ids.len() {
            return Ok(
                waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::AllRegistered,
            );
        }

        // Keys missing from `RETURNING` conflicted with existing
        // registrations and were left untouched; the rest were durably
        // registered above.
        let registered: std::collections::HashSet<InstanceId> =
            rows.iter().map(|row| row.get("workload_id")).collect();
        let already: Vec<InstanceId> = vm_ids
            .iter()
            .copied()
            .filter(|vm_id| !registered.contains(vm_id))
            .collect();
        Ok(
            waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::SomeAlreadyRegistered(
                nonempty_collections::NEVec::try_from_vec(already)
                    .expect("input ids are distinct (trait precondition), so fewer returned rows than inputs means at least one id conflicted"),
            ),
        )
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
