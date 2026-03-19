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
mod in_memory_backend;
mod stream_worker_pool;
mod utils;
mod workflow_store;

use self::bridge_service::*;
use self::in_memory_backend::*;
use self::stream_worker_pool::*;
use self::workflow_store::*;

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use waymark_proto::messages as proto;

const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:24117";

fn grpc_addr_from_env() -> SocketAddr {
    env::var("WAYMARK_BRIDGE_GRPC_ADDR")
        .unwrap_or_else(|_| DEFAULT_GRPC_ADDR.to_string())
        .parse()
        .expect("invalid WAYMARK_BRIDGE_GRPC_ADDR")
}

fn in_memory_mode() -> bool {
    env::var("WAYMARK_BRIDGE_IN_MEMORY")
        .ok()
        .map(|value| {
            let lowered = value.trim().to_ascii_lowercase();
            !lowered.is_empty() && lowered != "0" && lowered != "false" && lowered != "no"
        })
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "waymark=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let grpc_addr = grpc_addr_from_env();
    let in_memory = in_memory_mode();

    let store = if in_memory {
        None
    } else {
        let dsn = env::var("WAYMARK_DATABASE_URL").context("WAYMARK_DATABASE_URL must be set")?;
        Some(Arc::new(WorkflowStore::connect(&dsn).await?))
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
