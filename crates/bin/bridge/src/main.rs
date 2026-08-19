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
mod workflow_store;

use self::bridge_service::*;
use self::workflow_store::*;

use std::sync::Arc;

use color_eyre::eyre::WrapErr as _;
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
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let grpc_addr = envfury::or_parse("WAYMARK_BRIDGE_GRPC_ADDR", DEFAULT_GRPC_ADDR)?;
    let PermissiveBool(in_memory) = envfury::or_parse("WAYMARK_BRIDGE_IN_MEMORY", "false")?;

    let store = if in_memory {
        None
    } else {
        let dsn: SecretString = envfury::must("WAYMARK_DATABASE_URL")?;
        let workflow_store = WorkflowStore::connect(&dsn).await?;
        Some(Arc::new(workflow_store))
    };

    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let service = BridgeService { store };
    health_reporter
        .set_serving::<proto::workflow_service_server::WorkflowServiceServer<BridgeService>>()
        .await;

    info!(%grpc_addr, in_memory, "waymark bridge starting");

    let workflow_service = proto::workflow_service_server::WorkflowServiceServer::new(service)
        .max_decoding_message_size(waymark_proto::GRPC_MAX_MESSAGE_SIZE_BYTES)
        .max_encoding_message_size(waymark_proto::GRPC_MAX_MESSAGE_SIZE_BYTES);

    tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(workflow_service)
        .serve(grpc_addr)
        .await
        .wrap_err("bridge server exited")?;

    Ok(())
}
