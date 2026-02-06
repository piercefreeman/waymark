//! Start Workers - Runs the core runloop with Python worker pool.
//!
//! This binary starts the worker infrastructure:
//! - Connects to the database
//! - Starts the WorkerBridge gRPC server for worker connections
//! - Spawns a pool of Python workers
//! - Runs the core runloop to process queued workflow instances
//! - Optionally starts the scheduler and web dashboard
//!
//! Configuration is via environment variables:
//! - RAPPEL_DATABASE_URL: PostgreSQL connection string (required)
//! - RAPPEL_WORKER_GRPC_ADDR: gRPC server for worker connections (default: 127.0.0.1:24118)
//! - RAPPEL_USER_MODULE: Python module(s) to preload (comma-separated)
//! - RAPPEL_WORKER_COUNT: Number of workers (default: num_cpus)
//! - RAPPEL_CONCURRENT_PER_WORKER: Max concurrent actions per worker (default: 10)
//! - RAPPEL_POLL_INTERVAL_MS: Poll interval for queued instances (default: 100)
//! - RAPPEL_MAX_CONCURRENT_INSTANCES: Max workflow instances held concurrently (default: 50)
//! - RAPPEL_EXECUTOR_SHARDS: Executor shard thread count (default: num_cpus)
//! - RAPPEL_INSTANCE_DONE_BATCH_SIZE: Instance completion flush batch size (default: claim size)
//! - RAPPEL_PERSIST_INTERVAL_MS: Result persistence tick (default: 500)
//! - RAPPEL_MAX_ACTION_LIFECYCLE: Max actions per worker before recycling
//! - RAPPEL_SCHEDULER_POLL_INTERVAL_MS: Scheduler poll interval (default: 1000)
//! - RAPPEL_SCHEDULER_BATCH_SIZE: Scheduler batch size (default: 100)
//! - RAPPEL_WEBAPP_ENABLED / RAPPEL_WEBAPP_ADDR: Web dashboard configuration
//! - RAPPEL_RUNNER_PROFILE_INTERVAL_MS: Status reporting interval (default: 5000)

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use prost::Message;
use sqlx::{PgPool, Row};
use tokio::signal;
use tokio::sync::watch;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::backends::PostgresBackend;
use rappel::config::WorkerConfig;
use rappel::db;
use rappel::messages::ast as ir;
use rappel::rappel_core::dag::{DAG, convert_to_dag};
use rappel::rappel_core::runloop::{RunLoopSupervisorConfig, runloop_supervisor};
use rappel::{
    PythonWorkerConfig, RemoteWorkerPool, WebappServer, spawn_scheduler, spawn_status_reporter,
};
use uuid::Uuid;

type DagResolver = Arc<dyn Fn(&str) -> Option<DAG> + Send + Sync>;
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rappel=info,start_workers=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration and announce startup.
    let config = WorkerConfig::from_env()?;

    info!(
        worker_count = config.worker_count,
        concurrent_per_worker = config.concurrent_per_worker,
        user_modules = ?config.user_modules,
        executor_shards = config.executor_shards,
        max_action_lifecycle = ?config.max_action_lifecycle,
        "starting worker infrastructure"
    );

    // Initialize the database and backend.
    let pool = PgPool::connect(&config.database_url).await?;
    db::run_migrations(&pool).await?;
    let backend = PostgresBackend::new(pool);

    // Start the worker pool (bridge + python workers).
    let mut worker_config = PythonWorkerConfig::new();
    if !config.user_modules.is_empty() {
        worker_config = worker_config.with_user_modules(config.user_modules.clone());
    }

    let remote_pool = RemoteWorkerPool::new_with_config(
        worker_config,
        config.worker_count,
        Some(config.worker_grpc_addr),
        config.max_action_lifecycle,
        config.concurrent_per_worker,
    )
    .await?;
    info!(
        count = config.worker_count,
        bridge_addr = %remote_pool.bridge_addr(),
        "python worker pool started"
    );

    // Start the webapp server.
    let webapp_backend = Arc::new(backend.clone());
    let webapp_server = WebappServer::start(config.webapp.clone(), webapp_backend).await?;

    // Start the scheduler loop.
    let dag_resolver = build_dag_resolver(backend.pool().clone());
    let (scheduler_handle, scheduler_shutdown) =
        spawn_scheduler(backend.clone(), config.scheduler.clone(), dag_resolver);
    info!(
        poll_interval_ms = config.scheduler.poll_interval.as_millis(),
        batch_size = config.scheduler.batch_size,
        "scheduler task started"
    );

    // Wire shutdown coordination.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Start status reporting.
    let pool_id = Uuid::new_v4();
    let status_reporter_handle = spawn_status_reporter(
        pool_id,
        backend.clone(),
        remote_pool.clone(),
        config.profile_interval,
        shutdown_rx.clone(),
    );

    let shutdown_handle = tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let scheduler_shutdown = scheduler_shutdown.clone();
        async move {
            if let Err(err) = wait_for_shutdown().await {
                error!(error = %err, "shutdown signal listener failed");
                return;
            }
            info!("shutdown signal received");
            let _ = shutdown_tx.send(true);
            let _ = scheduler_shutdown.send(true);
        }
    });

    // Run the runloop supervisor until shutdown.
    runloop_supervisor(
        backend.clone(),
        remote_pool.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: config.max_concurrent_instances,
            executor_shards: config.executor_shards,
            instance_done_batch_size: config.instance_done_batch_size,
            poll_interval: config.poll_interval,
            persistence_interval: config.persistence_interval,
        },
        shutdown_rx,
    )
    .await;

    let _ = shutdown_handle.await;
    let _ = tokio::time::timeout(Duration::from_secs(5), scheduler_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), status_reporter_handle).await;

    if let Err(err) = remote_pool.shutdown().await {
        warn!(error = %err, "worker pool shutdown failed");
    }

    if let Some(webapp) = webapp_server {
        webapp.shutdown().await;
    }

    info!("shutdown complete");
    Ok(())
}

fn build_dag_resolver(pool: sqlx::PgPool) -> DagResolver {
    Arc::new(move |workflow_name| {
        let pool = pool.clone();
        let workflow_name = workflow_name.to_string();
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move {
                let row = sqlx::query(
                    r#"
                    SELECT program_proto
                    FROM workflow_versions
                    WHERE workflow_name = $1
                    ORDER BY created_at DESC
                    LIMIT 1
                    "#,
                )
                .bind(&workflow_name)
                .fetch_optional(&pool)
                .await
                .ok()??;

                let payload: Vec<u8> = row.get("program_proto");
                let program = ir::Program::decode(&payload[..]).ok()?;
                convert_to_dag(&program).ok()
            })
        })
    })
}

async fn wait_for_shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal as unix_signal};

        let mut terminate = unix_signal(SignalKind::terminate())?;
        tokio::select! {
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
