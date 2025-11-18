use std::time::Duration;

use anyhow::{Context, Result};
use prost::Message;
use sha2::{Digest, Sha256};
use sqlx::{Row, postgres::PgRow};
use tokio::{runtime::Runtime, time::sleep};

use crate::{db::Database, messages::proto::WorkflowRegistration};

pub async fn run_instance_payload(database_url: &str, payload: &[u8]) -> Result<i64> {
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
    Ok(version_id)
}

pub async fn wait_for_instance_poll(
    database_url: &str,
    poll_interval: Duration,
) -> Result<Option<Vec<u8>>> {
    let db = Database::connect(database_url).await?;
    loop {
        let payload =
            sqlx::query("SELECT payload FROM daemon_action_ledger ORDER BY id DESC LIMIT 1")
                .map(|row: PgRow| row.get(0))
                .fetch_optional(db.pool())
                .await?;
        if payload.is_some() {
            return Ok(payload);
        }
        sleep(poll_interval).await;
    }
}

pub fn run_instance_blocking(database_url: &str, payload: Vec<u8>) -> Result<i64> {
    let rt = Runtime::new()?;
    rt.block_on(run_instance_payload(database_url, &payload))
}

pub fn wait_for_instance_blocking(
    database_url: &str,
    poll_interval: Duration,
) -> Result<Option<Vec<u8>>> {
    let rt = Runtime::new()?;
    rt.block_on(wait_for_instance_poll(database_url, poll_interval))
}
