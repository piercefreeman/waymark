use std::time::Duration;

use anyhow::{Context, Result};
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::{runtime::Runtime, time::sleep};

use crate::{
    WorkflowInstanceId, WorkflowVersionId, db::Database, messages::proto::WorkflowRegistration,
    ir_to_dag::convert_workflow,
};

pub async fn run_instance_payload(
    database_url: &str,
    payload: &[u8],
) -> Result<(WorkflowVersionId, WorkflowInstanceId)> {
    let registration = WorkflowRegistration::decode(payload)
        .context("failed to decode workflow registration payload")?;

    // Get the DAG - either from IR (preferred) or legacy dag field
    let (dag_def, from_ir) = if let Some(ref ir) = registration.ir {
        // Convert IR to DAG
        let dag = convert_workflow(ir, registration.concurrent)
            .map_err(|e| anyhow::anyhow!("failed to convert IR to DAG: {}", e))?;
        (dag, true)
    } else if let Some(dag) = registration.dag.clone() {
        // Legacy path: use provided DAG directly
        (dag, false)
    } else {
        anyhow::bail!("workflow registration missing both ir and dag");
    };

    let dag_bytes = dag_def.encode_to_vec();
    let provided_hash = &registration.dag_hash;
    let computed_hash = format!("{:x}", Sha256::digest(&dag_bytes));

    // For IR-based registrations, the hash is of the IR not the DAG,
    // so we just use the computed DAG hash
    let hash_to_use = if from_ir {
        computed_hash.clone()
    } else {
        // Legacy path: warn if hashes don't match
        if !provided_hash.is_empty() && provided_hash != &computed_hash {
            tracing::warn!(
                provided = provided_hash,
                computed = computed_hash,
                "workflow hash mismatch - using computed value"
            );
        }
        computed_hash
    };

    let db = Database::connect(database_url).await?;
    let version_id = db
        .upsert_workflow_version(
            &registration.workflow_name,
            &hash_to_use,
            &dag_bytes,
            dag_def.concurrent,
        )
        .await?;
    // Serialize initial_context if provided
    let input_payload = registration.initial_context.map(|ctx| ctx.encode_to_vec());
    let instance_id = db
        .create_workflow_instance(
            &registration.workflow_name,
            version_id,
            input_payload.as_deref(),
        )
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
