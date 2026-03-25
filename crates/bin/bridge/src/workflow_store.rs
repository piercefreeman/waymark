use std::time::Duration;

use anyhow::Result;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use waymark_backend_postgres::PostgresBackend;
use waymark_core_backend::QueuedInstance;
use waymark_proto::messages as proto;
use waymark_secret_string::SecretStr;
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend};

#[derive(Clone)]
pub struct WorkflowStore {
    pub backend: PostgresBackend,
}

impl WorkflowStore {
    pub async fn connect(dsn: &SecretStr) -> Result<Self> {
        let pool = PgPool::connect(dsn.expose_secret()).await?;
        waymark_backend_postgres_migrations::run(&pool).await?;
        let backend = PostgresBackend::new(pool);
        Ok(Self { backend })
    }

    fn pool(&self) -> &sqlx::PgPool {
        self.backend.pool()
    }

    pub async fn upsert_workflow_version(
        &self,
        registration: &proto::WorkflowRegistration,
    ) -> Result<Uuid> {
        let workflow_version = if registration.workflow_version.is_empty() {
            registration.ir_hash.clone()
        } else {
            registration.workflow_version.clone()
        };
        let backend_registration = WorkflowRegistration {
            workflow_name: registration.workflow_name.clone(),
            workflow_version,
            ir_hash: registration.ir_hash.clone(),
            program_proto: registration.ir.clone(),
            concurrent: registration.concurrent,
        };
        self.backend
            .upsert_workflow_version(&backend_registration)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }

    pub async fn queue_instances(&self, instances: &[QueuedInstance]) -> Result<()> {
        self.backend
            .queue_instances(instances)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }

    pub async fn wait_for_instance(
        &self,
        instance_id: Uuid,
        poll_interval: Duration,
    ) -> Result<Option<proto::WorkflowArguments>> {
        loop {
            let row = sqlx::query(
                r#"
                SELECT result, error
                FROM runner_instances
                WHERE instance_id = $1
                "#,
            )
            .bind(instance_id)
            .fetch_optional(self.pool())
            .await?;

            let Some(row) = row else {
                return Ok(None);
            };

            let result_bytes: Option<Vec<u8>> = row.get("result");
            let error_bytes: Option<Vec<u8>> = row.get("error");

            if result_bytes.is_some() || error_bytes.is_some() {
                let result_value = result_bytes
                    .as_deref()
                    .map(rmp_serde::from_slice::<serde_json::Value>)
                    .transpose()
                    .map_err(|err| anyhow::anyhow!(err))?;
                let error_value = error_bytes
                    .as_deref()
                    .map(rmp_serde::from_slice::<serde_json::Value>)
                    .transpose()
                    .map_err(|err| anyhow::anyhow!(err))?;

                return Ok(Some(crate::utils::build_workflow_arguments(
                    result_value,
                    error_value,
                )));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}
