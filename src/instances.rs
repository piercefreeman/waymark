//! Instance management utilities.
//!
//! This module provides helpers for running workflow instances.

use std::time::Duration;

use anyhow::{Context, Result};
use prost::Message;
use sqlx::postgres::PgPoolOptions;
use tokio::{runtime::Runtime, time::sleep};

use crate::{
    WorkflowInstanceId, WorkflowVersionId,
    messages::proto::WorkflowRegistration,
    store::Store,
};

/// Run a workflow instance from a serialized registration payload
pub async fn run_instance_payload(
    database_url: &str,
    payload: &[u8],
) -> Result<(WorkflowVersionId, WorkflowInstanceId)> {
    let registration = WorkflowRegistration::decode(payload)
        .context("failed to decode workflow registration payload")?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .context("failed to connect to database")?;

    let store = Store::new(pool);
    store.init_schema().await?;

    let (version_id, instance_id) = store.register_workflow(&registration).await?;

    Ok((version_id, instance_id))
}

/// Poll for instance completion
pub async fn wait_for_instance_poll(
    database_url: &str,
    instance_id: WorkflowInstanceId,
    poll_interval: Duration,
) -> Result<Option<Vec<u8>>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .context("failed to connect to database")?;

    let store = Store::new(pool);

    loop {
        match store.get_instance(instance_id).await? {
            Some((status, result)) if status == "completed" => {
                return Ok(result.map(|v| serde_json::to_vec(&v).unwrap_or_default()));
            }
            Some((status, _)) if status == "failed" => {
                return Ok(None);
            }
            Some(_) => {
                // Still running
                sleep(poll_interval).await;
            }
            None => {
                return Ok(None);
            }
        }
    }
}

/// Blocking version of run_instance_payload
pub fn run_instance_blocking(
    database_url: &str,
    payload: Vec<u8>,
) -> Result<(WorkflowVersionId, WorkflowInstanceId)> {
    let rt = Runtime::new()?;
    rt.block_on(run_instance_payload(database_url, &payload))
}

/// Blocking version of wait_for_instance_poll
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
