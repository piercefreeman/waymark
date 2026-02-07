use sqlx::Row;
use tonic::async_trait;
use uuid::Uuid;

use super::PostgresBackend;
use crate::backends::base::{
    BackendError, BackendResult, WorkflowRegistration, WorkflowRegistryBackend, WorkflowVersion,
};

#[async_trait]
impl WorkflowRegistryBackend for PostgresBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let inserted = sqlx::query(
            r#"
            INSERT INTO workflow_versions
                (workflow_name, workflow_version, ir_hash, program_proto, concurrent)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (workflow_name, workflow_version)
            DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&registration.workflow_name)
        .bind(&registration.workflow_version)
        .bind(&registration.ir_hash)
        .bind(&registration.program_proto)
        .bind(registration.concurrent)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = inserted {
            let id: Uuid = row.get("id");
            return Ok(id);
        }

        let row = sqlx::query(
            r#"
            SELECT id, ir_hash
            FROM workflow_versions
            WHERE workflow_name = $1 AND workflow_version = $2
            "#,
        )
        .bind(&registration.workflow_name)
        .bind(&registration.workflow_version)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        let existing_hash: String = row.get("ir_hash");
        if existing_hash != registration.ir_hash {
            return Err(BackendError::Message(format!(
                "workflow version already exists with different IR hash: {}@{}",
                registration.workflow_name, registration.workflow_version
            )));
        }

        Ok(id)
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_name, workflow_version, ir_hash, program_proto, concurrent
            FROM workflow_versions
            WHERE id = ANY($1)
            "#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut versions = Vec::with_capacity(rows.len());
        for row in rows {
            versions.push(WorkflowVersion {
                id: row.get("id"),
                workflow_name: row.get("workflow_name"),
                workflow_version: row.get("workflow_version"),
                ir_hash: row.get("ir_hash"),
                program_proto: row.get("program_proto"),
                concurrent: row.get("concurrent"),
            });
        }
        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::super::test_helpers::setup_backend;
    use crate::backends::{WorkflowRegistration, WorkflowRegistryBackend};

    fn sample_registration(version: &str) -> WorkflowRegistration {
        WorkflowRegistration {
            workflow_name: "tests.workflow".to_string(),
            workflow_version: version.to_string(),
            ir_hash: format!("hash-{version}"),
            program_proto: vec![1, 2, 3, 4],
            concurrent: true,
        }
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn workflow_registry_upsert_workflow_version_happy_path() {
        let backend = setup_backend().await;
        let registration = sample_registration("v1");

        let id = WorkflowRegistryBackend::upsert_workflow_version(&backend, &registration)
            .await
            .expect("insert workflow version");
        let repeat_id = WorkflowRegistryBackend::upsert_workflow_version(&backend, &registration)
            .await
            .expect("idempotent workflow upsert");

        assert_eq!(id, repeat_id);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn workflow_registry_get_workflow_versions_happy_path() {
        let backend = setup_backend().await;
        let registration = sample_registration("v2");
        let id = WorkflowRegistryBackend::upsert_workflow_version(&backend, &registration)
            .await
            .expect("insert workflow version");

        let versions = WorkflowRegistryBackend::get_workflow_versions(&backend, &[id])
            .await
            .expect("get workflow versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].id, id);
        assert_eq!(versions[0].workflow_name, registration.workflow_name);
        assert_eq!(versions[0].workflow_version, registration.workflow_version);
        assert_eq!(versions[0].ir_hash, registration.ir_hash);
        assert_eq!(versions[0].program_proto, registration.program_proto);
        assert_eq!(versions[0].concurrent, registration.concurrent);
    }
}
