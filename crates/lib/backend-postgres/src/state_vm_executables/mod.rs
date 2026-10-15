//! Postgres backend for VM executable loading.
//!
//! Implements [`waymark_state_vm_executables_backend::LoadExecutable`]
//! for [`crate::PostgresBackend`].

pub mod error;

#[cfg(test)]
mod tests;

use sqlx::Row;
use waymark_ids::WorkflowVersionId;
use waymark_observability::obs;

use crate::PostgresBackend;

impl waymark_state_vm_executables_backend::HasExecutableId for PostgresBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_state_vm_executables_backend::LoadExecutable for PostgresBackend {
    type Error = error::LoadError;

    #[obs]
    #[function_name::named]
    async fn load_executable<'a>(
        &'a self,
        id: &'a Self::ExecutableId,
    ) -> Result<waymark_state_vm_executables_backend::LoadedExecutable, Self::Error> {
        Self::count_query(&self.query_counts, "select:vm_executables_by_id");
        let row = sqlx::query(
            r#"
            SELECT name, version, bytecode FROM vm_executables
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(error::LoadError::NotFound(*id))?;

        Ok(waymark_state_vm_executables_backend::LoadedExecutable {
            name: row.get("name"),
            version: row.get("version"),
            data: row.get("bytecode"),
        })
    }
}
