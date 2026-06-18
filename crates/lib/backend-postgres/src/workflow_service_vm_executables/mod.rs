//! Postgres backend for the workflow service VM executables.
//!
//! Implements [`waymark_workflow_service_vm_executables_backend::UpsertExecutable`]
//! for [`crate::PostgresBackend`].

pub mod error;

#[cfg(test)]
mod tests;

use sqlx::Row;
use waymark_ids::WorkflowVersionId;
use waymark_observability::obs;

use crate::PostgresBackend;

impl waymark_workflow_service_vm_executables_backend::HasExecutableId for PostgresBackend {
    type ExecutableId = WorkflowVersionId;
}

impl waymark_workflow_service_vm_executables_backend::UpsertExecutable for PostgresBackend {
    type Error = error::UpsertError;

    #[obs]
    #[function_name::named]
    async fn upsert_executable<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        bytes: &'a [u8],
    ) -> Result<Self::ExecutableId, Self::Error> {
        let id = WorkflowVersionId::new_uuid_v4();
        Self::count_query(&self.query_counts, "upsert:vm_executables");

        let row = sqlx::query(
            r#"
            WITH
              inserted AS (
                INSERT INTO vm_executables (id, name, version, bytecode)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (name, version) DO NOTHING
                RETURNING id
              ),
              existing AS (
                SELECT id, bytecode FROM vm_executables
                WHERE name = $2 AND version = $3
                  AND NOT EXISTS (SELECT 1 FROM inserted)
              )
            SELECT id, NULL::bytea AS bytecode, TRUE AS is_new FROM inserted
            UNION ALL
            SELECT id, bytecode, FALSE AS is_new FROM existing
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(version)
        .bind(bytes)
        .fetch_one(&self.pool)
        .await?;

        let is_new: bool = row.get("is_new");
        if is_new {
            let returned: WorkflowVersionId = row.get("id");
            return Ok(returned);
        }

        let existing_bytes: Vec<u8> = row.get("bytecode");
        if existing_bytes == bytes {
            let existing_id: WorkflowVersionId = row.get("id");
            Ok(existing_id)
        } else {
            Err(error::UpsertError::Conflict)
        }
    }
}
