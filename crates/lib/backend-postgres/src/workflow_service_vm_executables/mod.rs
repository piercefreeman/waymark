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

        // `DO UPDATE` with a no-op set (rather than `DO NOTHING`) guarantees a
        // row is always returned. Under READ COMMITTED, `DO NOTHING` can detect
        // a conflict against a concurrently-committed duplicate that a
        // same-statement `SELECT` — bound to the statement snapshot — can't yet
        // see, yielding zero rows. `DO UPDATE` re-reads and locks the
        // conflicting row, so `RETURNING` always produces exactly one row.
        //
        // The set is a no-op, so the row's stored bytecode is preserved. The
        // `bytecode = $4` comparison runs server-side so the (potentially large)
        // blob never travels back — we only need to know whether it matches. For
        // a fresh insert the stored bytecode is the one we just wrote, so
        // `matches` is trivially true.
        let row = sqlx::query(
            r#"
            INSERT INTO vm_executables (id, name, version, bytecode)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name, version) DO UPDATE SET name = EXCLUDED.name
            RETURNING id, bytecode = $4 AS matches
            "#,
        )
        .bind(id)
        .bind(name)
        .bind(version)
        .bind(bytes)
        .fetch_one(&self.pool)
        .await?;

        if row.get::<bool, _>("matches") {
            let returned_id: WorkflowVersionId = row.get("id");
            Ok(returned_id)
        } else {
            Err(error::UpsertError::Conflict)
        }
    }
}
