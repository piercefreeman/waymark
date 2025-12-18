//! Rappel Bridge - gRPC server for workflow registration and singleton discovery.
//!
//! This binary starts the Rappel bridge server with:
//! - gRPC WorkflowService for workflow registration
//! - gRPC health check for singleton discovery
//!
//! Configuration is via environment variables:
//! - RAPPEL_DATABASE_URL: PostgreSQL connection string (required)
//! - RAPPEL_BRIDGE_GRPC_ADDR: gRPC server bind address (default: 127.0.0.1:24117)

use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::{get_config, server_client};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rappel=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration from global cache
    let config = get_config();

    let server_config = server_client::ServerConfig {
        grpc_addr: config.bridge_grpc_addr,
        database_url: config.database_url,
    };

    // Run the server
    server_client::run_server(server_config).await
}
