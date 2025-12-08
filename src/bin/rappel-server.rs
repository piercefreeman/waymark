//! Main entry point for the rappel server.
//!
//! Starts a single HostCoordinator with configuration from environment variables.

use std::sync::Arc;

use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::{config::Config, coordinator::HostConfig, db::Database, HostCoordinator};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting rappel server");

    // Load configuration
    let config = Config::from_env()?;
    info!(?config, "Loaded configuration");

    // Connect to database
    let db = Arc::new(Database::connect(&config.database_url).await?);
    info!("Connected to database");

    // Run migrations
    db.migrate().await?;
    info!("Database migrations complete");

    // Create host config from environment config
    let host_config = HostConfig {
        http_addr: config.http_addr,
        action_grpc_addr: config.action_grpc_addr,
        instance_grpc_addr: config.instance_grpc_addr,
        action_worker_count: config.action_worker_count,
        instance_worker_count: config.instance_worker_count,
        max_concurrent_per_action_worker: config.max_concurrent_per_action_worker,
        max_concurrent_per_instance_worker: config.max_concurrent_per_instance_worker,
        user_modules: config.user_modules,
        poll_interval_ms: 50,
        python_dir: config.python_dir,
    };

    // Create and start coordinator
    let mut coordinator = HostCoordinator::new(host_config, db);
    coordinator.start().await?;

    info!("Rappel server started, press Ctrl+C to stop");

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;

    info!("Shutdown signal received");
    coordinator.stop().await;

    Ok(())
}
