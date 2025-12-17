//! Rappel Server - Main entry point for the workflow engine.
//!
//! This binary starts the Rappel server with:
//! - HTTP API for workflow registration
//! - gRPC server for Python worker connections
//!
//! Configuration is via environment variables:
//! - RAPPEL_DATABASE_URL: PostgreSQL connection string (required)
//! - RAPPEL_HTTP_ADDR: HTTP server bind address (default: 127.0.0.1:24117)
//! - RAPPEL_GRPC_ADDR: gRPC server bind address (default: HTTP port + 1)

use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::{get_config, server_client};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rappel=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration from global cache
    let config = get_config();

    let server_config = server_client::ServerConfig {
        http_addr: config.http_addr,
        grpc_addr: config.grpc_addr,
        database_url: config.database_url,
    };

    // Run the servers
    server_client::run_servers(server_config).await
}
