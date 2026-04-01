//! Waymark Bridge - gRPC server for workflow registration and singleton discovery.
//!
//! This binary starts the Waymark bridge server with:
//! - gRPC WorkflowService for workflow registration
//! - gRPC health check for singleton discovery
//!
//! Configuration is via environment variables:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string (required unless in-memory)
//! - WAYMARK_BRIDGE_GRPC_ADDR: gRPC server bind address (default: 127.0.0.1:24117)
//! - WAYMARK_BRIDGE_IN_MEMORY: enable in-memory execution mode for streaming workflows

mod bridge_service;
mod stream_worker_pool;
mod utils;
mod workflow_store;

use self::bridge_service::*;
use self::stream_worker_pool::*;
use self::workflow_store::*;

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;
use waymark_secret_string::SecretString;

use waymark_proto::messages as proto;

const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:24117";

struct PermissiveBool(pub bool);

impl core::str::FromStr for PermissiveBool {
    type Err = core::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let lowered = s.trim().to_ascii_lowercase();
        let val = !lowered.is_empty() && lowered != "0" && lowered != "false" && lowered != "no";
        Ok(Self(val))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    waymark_fn_main_common::init_tracing()?;

    let grpc_addr = envfury::or_parse("WAYMARK_BRIDGE_GRPC_ADDR", DEFAULT_GRPC_ADDR)?;
    let PermissiveBool(in_memory) = envfury::or_parse("WAYMARK_BRIDGE_IN_MEMORY", "false")?;

    let store = if in_memory {
        None
    } else {
        let dsn: SecretString = envfury::must("WAYMARK_DATABASE_URL")?;
        let workflow_store = WorkflowStore::connect(&dsn).await?;
        Some(Arc::new(workflow_store))
    };

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    let service = BridgeService { store };
    health_reporter
        .set_serving::<proto::workflow_service_server::WorkflowServiceServer<BridgeService>>()
        .await;

    info!(%grpc_addr, in_memory, "waymark bridge starting");

    tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(proto::workflow_service_server::WorkflowServiceServer::new(
            service,
        ))
        .serve(grpc_addr)
        .await
        .context("bridge server exited")?;

    Ok(())
}
