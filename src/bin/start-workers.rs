//! Start Workers - Runs the instance runner with Python worker pool.
//!
//! This binary starts the worker infrastructure:
//! - Connects to the database
//! - Starts the WorkerBridge gRPC server for worker connections
//! - Spawns a pool of Python workers
//! - Runs the InstanceRunner to process workflow instances
//! - Optionally starts the web dashboard for monitoring
//!
//! Configuration is via environment variables:
//! - RAPPEL_DATABASE_URL: PostgreSQL connection string (required)
//! - RAPPEL_WORKER_GRPC_ADDR: gRPC server for worker connections (default: 127.0.0.1:24118)
//! - RAPPEL_USER_MODULE: Python module to preload
//! - RAPPEL_WORKER_COUNT: Number of workers (default: num_cpus)
//! - RAPPEL_WEBAPP_ENABLED: Set to "true" or "1" to enable web dashboard
//! - RAPPEL_WEBAPP_ADDR: Web dashboard address (default: 0.0.0.0:24119)

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::{select, signal};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::{
    Database, InstanceRunner, InstanceRunnerConfig, PythonWorkerConfig, PythonWorkerPool,
    WebappServer, WorkerBridgeServer, get_config,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rappel=info,start_workers=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration from global cache
    let config = get_config();

    info!(
        worker_count = config.worker_count,
        concurrent_per_worker = config.concurrent_per_worker,
        user_module = ?config.user_module,
        max_action_lifecycle = ?config.max_action_lifecycle,
        "starting worker pool"
    );

    // Connect to database
    let database =
        Database::connect_with_pool_size(&config.database_url, config.db_max_connections).await?;
    info!("connected to database");

    let webapp_database = if config.webapp.enabled {
        Some(Arc::new(
            Database::connect_with_pool_size(
                &config.database_url,
                config.webapp.db_max_connections,
            )
            .await?,
        ))
    } else {
        None
    };

    // Start worker bridge server
    let worker_bridge = WorkerBridgeServer::start(Some(config.worker_grpc_addr)).await?;
    info!(addr = %worker_bridge.addr(), "worker bridge started");

    // Start webapp server if enabled
    let webapp_server = match webapp_database {
        Some(db) => WebappServer::start(config.webapp.clone(), db).await?,
        None => WebappServer::start(config.webapp.clone(), Arc::new(database.clone())).await?,
    };

    // Configure Python workers
    let mut worker_config = PythonWorkerConfig::new();
    if let Some(module) = &config.user_module {
        worker_config = worker_config.with_user_module(module);
    }

    // Create worker pool with concurrency limit
    let worker_pool = Arc::new(
        PythonWorkerPool::new_with_concurrency(
            worker_config,
            config.worker_count,
            Arc::clone(&worker_bridge),
            config.max_action_lifecycle,
            config.concurrent_per_worker,
        )
        .await?,
    );
    info!(
        worker_count = config.worker_count,
        "python worker pool started"
    );

    // Configure and create instance runner
    let runner_config = InstanceRunnerConfig {
        claim_batch_size: config.batch_size as i32,
        completion_batch_size: config.completion_batch_size,
        idle_poll_interval: Duration::from_millis(config.poll_interval_ms),
        schedule_check_interval: Duration::from_millis(config.schedule_check_interval_ms),
        schedule_check_batch_size: config.schedule_check_batch_size,
        gc_interval: config.gc.interval_ms.map(Duration::from_millis),
        gc_retention_seconds: config.gc.retention_seconds,
        gc_batch_size: config.gc.batch_size,
        ..Default::default()
    };

    let runner = Arc::new(InstanceRunner::new(
        runner_config,
        database,
        Arc::clone(&worker_pool),
    ));

    info!(
        claim_batch_size = config.batch_size,
        poll_interval_ms = config.poll_interval_ms,
        "instance runner created - waiting for shutdown signal"
    );

    // Spawn runner in background task
    let runner_clone = Arc::clone(&runner);
    let runner_handle = tokio::spawn(async move {
        if let Err(e) = runner_clone.run().await {
            tracing::error!("Instance runner error: {}", e);
        }
    });

    // Wait for shutdown signal
    wait_for_shutdown().await?;
    info!("shutdown signal received - stopping workers");

    // Shutdown runner
    runner.shutdown();

    // Wait for runner to finish (with timeout)
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), runner_handle).await;

    // Shutdown worker pool
    drop(runner); // Release Arc reference
    let pool = Arc::try_unwrap(worker_pool)
        .map_err(|_| anyhow!("worker pool still referenced during shutdown"))?;
    pool.shutdown().await?;

    // Shutdown worker bridge
    worker_bridge.shutdown().await;

    // Shutdown webapp server if running
    if let Some(webapp) = webapp_server {
        webapp.shutdown().await;
    }

    info!("shutdown complete");
    Ok(())
}

async fn wait_for_shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal as unix_signal};

        let mut terminate = unix_signal(SignalKind::terminate())?;
        select! {
            _ = signal::ctrl_c() => {
                info!("Ctrl+C received");
            }
            _ = terminate.recv() => {
                info!("SIGTERM received");
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        info!("Ctrl+C received");
        Ok(())
    }
}
