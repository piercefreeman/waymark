use std::time::Duration;

use anyhow::{Context, Result};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::{runtime::Runtime, time::sleep};

use crate::{
    WorkflowInstanceId, WorkflowVersionId, db::Database, messages::proto::WorkflowRegistration,
};

pub async fn run_instance_payload(
    database_url: &str,
    payload: &[u8],
) -> Result<(WorkflowVersionId, WorkflowInstanceId)> {
    let registration = WorkflowRegistration::decode(payload)
        .context("failed to decode workflow registration payload")?;
    let dag_def = registration
        .dag
        .context("workflow registration missing dag definition")?;
    let dag_bytes = dag_def.encode_to_vec();
    let provided_hash = registration.dag_hash;
    let computed_hash = format!("{:x}", Sha256::digest(&dag_bytes));
    if !provided_hash.is_empty() && provided_hash != computed_hash {
        tracing::warn!(
            provided = provided_hash,
            computed = computed_hash,
            "workflow hash mismatch - using computed value"
        );
    }
    let db = Database::connect(database_url).await?;
    let version_id = db
        .upsert_workflow_version(
            &registration.workflow_name,
            &computed_hash,
            &dag_bytes,
            dag_def.concurrent,
        )
        .await?;
    let instance_id = db
        .create_workflow_instance(&registration.workflow_name, version_id, None)
        .await?;
    Ok((version_id, instance_id))
}

pub async fn wait_for_instance_poll(
    database_url: &str,
    instance_id: WorkflowInstanceId,
    poll_interval: Duration,
) -> Result<Option<Vec<u8>>> {
    let db = Database::connect(database_url).await?;
    loop {
        let payload: Option<Option<Vec<u8>>> =
            sqlx::query_scalar("SELECT result_payload FROM workflow_instances WHERE id = $1")
                .bind(instance_id)
                .fetch_optional(db.pool())
                .await?;
        match payload {
            Some(Some(bytes)) => return Ok(Some(bytes)),
            Some(None) => sleep(poll_interval).await,
            None => return Ok(None),
        }
    }
}

pub fn run_instance_blocking(
    database_url: &str,
    payload: Vec<u8>,
) -> Result<(WorkflowVersionId, WorkflowInstanceId)> {
    let rt = Runtime::new()?;
    rt.block_on(run_instance_payload(database_url, &payload))
}

pub fn wait_for_instance_blocking(
    database_url: &str,
    instance_id: WorkflowInstanceId,
    poll_interval: Duration,
) -> Result<Option<Vec<u8>>> {
    let rt = Runtime::new()?;
    rt.block_on(wait_for_instance_poll(
        database_url,
        instance_id,
        poll_interval,
    ))
}
