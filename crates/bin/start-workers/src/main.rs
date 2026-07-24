//! Start Workers - Runs the VM execution subsystem with action worker pools.
//!
//! This binary starts the worker infrastructure:
//! - Connects to the database
//! - Starts the WorkerBridge gRPC server for worker connections
//! - Spawns Python workers and, when configured, JavaScript workers
//! - Runs the VM execution subsystem for queued workflow instances
//! - Optionally starts the scheduler and web dashboard
//!
//! Configuration is via environment variables:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string (required)
//! - WAYMARK_WORKER_GRPC_ADDR: gRPC server for worker connections (default: 127.0.0.1:24118)
//! - WAYMARK_USER_MODULE: Python module(s) to preload (comma-separated)
//! - WAYMARK_WORKER_COUNT: Number of workers (default: num_cpus)
//! - WAYMARK_CONCURRENT_PER_WORKER: Max concurrent actions per worker (default: 10)
//! - WAYMARK_JAVASCRIPT_WORKER_COUNT: JavaScript worker count (enables JavaScript)
//! - WAYMARK_JAVASCRIPT_ACTION_BUNDLE: Generated ESM action bundle
//! - WAYMARK_JAVASCRIPT_WORKER_GRPC_ADDR: JavaScript bridge address
//! - WAYMARK_JAVASCRIPT_WORKER_COMMAND: Node worker command
//! - WAYMARK_POLL_INTERVAL_MS: Poll interval for queued instances (default: 100)
//! - WAYMARK_MAX_CONCURRENT_INSTANCES: Max workflow instances held concurrently (default: 500)
//! - WAYMARK_EXECUTOR_SHARDS: Executor shard thread count (default: num_cpus)
//! - WAYMARK_INSTANCE_DONE_BATCH_SIZE: Instance completion flush batch size (default: claim size)
//! - WAYMARK_PERSIST_INTERVAL_MS: Result persistence tick (default: 500)
//! - WAYMARK_LOCK_TTL_MS: Instance lock TTL (default: 15000)
//! - WAYMARK_LOCK_HEARTBEAT_MS: Lock refresh heartbeat interval (default: 5000)
//! - WAYMARK_EVICT_SLEEP_THRESHOLD_MS: Sleep duration before evicting idle instances (default: 10000)
//! - WAYMARK_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS: Sweep interval for expired queue locks (default: 15000)
//! - WAYMARK_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE: Max expired locks to reclaim per sweep (default: 1000)
//! - WAYMARK_MAX_ACTION_LIFECYCLE: Max actions per worker before recycling
//! - WAYMARK_SCHEDULER_POLL_INTERVAL_MS: Scheduler poll interval (default: 1000)
//! - WAYMARK_SCHEDULER_BATCH_SIZE: Scheduler batch size (default: 100)
//! - WAYMARK_GARBAGE_COLLECTOR_INTERVAL_MS: Garbage collector interval (default: 300000)
//! - WAYMARK_GARBAGE_COLLECTOR_BATCH_SIZE: Garbage collector batch size (default: 100)
//! - WAYMARK_GARBAGE_COLLECTOR_RETENTION_HOURS: Done-instance retention window (default: 24)
//! - WAYMARK_WEBAPP_ENABLED / WAYMARK_WEBAPP_ADDR: Web dashboard configuration
//! - WAYMARK_RUNNER_PROFILE_INTERVAL_MS: Status reporting interval (default: 5000)

use std::sync::{Arc, atomic::AtomicUsize};
use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use tokio::signal;
use tracing::{error, info, warn};
use uuid::Uuid;

use waymark_backend_postgres::PostgresBackend;
use waymark_config::WorkerConfig;

#[tokio::main]
async fn main() -> Result<()> {
    waymark_fn_main_common::init()?;

    let metrics_addr: std::net::SocketAddr = envfury::or_parse("METRICS_ADDR", "0.0.0.0:9118")?;
    waymark_prometheus_exporter_bringup::spawn_and_install_recorder(metrics_addr)?;

    let _task_monitor = waymark_tokio_metrics_bringup::bringup(env!("CARGO_BIN_NAME"));

    // Load configuration and announce startup.
    let config = WorkerConfig::from_env()?;

    tracing::debug!(target: "raw-config", ?config, "raw config");

    info!(
        worker_count = config.worker_count,
        concurrent_per_worker = config.concurrent_per_worker,
        user_modules = ?config.user_modules,
        javascript_worker_count = config
            .javascript_worker
            .as_ref()
            .map(|worker| worker.worker_count.get()),
        "starting worker infrastructure"
    );

    metrics::gauge!(
        "waymark_start_workers_up",
        "worker_count" => config.worker_count.to_string(),
        "concurrent_per_worker" => config.concurrent_per_worker.to_string(),
        "user_modules" => format!("{:?}", config.user_modules),
        "max_action_lifecycle" => config.max_action_lifecycle.map(|val| val.to_string()).unwrap_or("no".into()),
        "max_concurrent_instances" => config.max_concurrent_instances.to_string(),
        "pinning_ttl_seconds" => config.lock_ttl.as_secs_f64().to_string(),
        "pinning_heartbeat_seconds" => config.lock_heartbeat.as_secs_f64().to_string(),
        "vm_retention_seconds" => config.vm_retention.as_secs_f64().to_string(),
        "vm_sweep_interval_seconds" => config.vm_sweep_interval.as_secs_f64().to_string(),
        "executable_retention_seconds" => config.executable_retention.as_secs_f64().to_string(),
        "executable_sweep_interval_seconds" => config.executable_sweep_interval.as_secs_f64().to_string(),
        "profile_interval_seconds" => config.profile_interval.as_secs_f64().to_string(),
    )
    .set(1);

    // Wire shutdown coordination.
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();

    // Initialize the database and backend.
    let pool = PgPool::connect(config.database_url.expose_secret()).await?;
    waymark_backend_postgres_migrations::run(&pool).await?;
    let backend = PostgresBackend::new(pool);

    // Start the Python worker pool.
    let mut worker_config = waymark_worker_python::Config::new();
    if !config.user_modules.is_empty() {
        worker_config = worker_config.with_user_modules(config.user_modules.clone());
    }

    let worker_process_spec_builder = |bridge_server_addr| waymark_worker_python::Spec {
        bridge_server_addr,
        config: worker_config,
    };

    let (python_process_pool, python_bridge_task) = waymark_worker_remote_bringup::start(
        shutdown_token.clone(),
        Some(config.worker_grpc_addr),
        worker_process_spec_builder,
        config.worker_count,
        config.max_action_lifecycle,
        config.concurrent_per_worker,
    )
    .await?;

    let python_process_pool = Arc::new(python_process_pool);

    // JavaScript is opt-in: both count and generated bundle are required.
    let (javascript_process_pool, javascript_bridge_task) =
        if let Some(javascript_worker) = &config.javascript_worker {
            let worker_config =
                waymark_worker_javascript::Config::new(javascript_worker.action_bundle.clone())
                    .with_command(javascript_worker.command.clone());
            let (pool, bridge_task) = waymark_worker_remote_bringup::start(
                shutdown_token.clone(),
                Some(javascript_worker.grpc_addr),
                |bridge_server_addr| waymark_worker_javascript::Spec {
                    bridge_server_addr,
                    config: worker_config,
                },
                javascript_worker.worker_count,
                config.max_action_lifecycle,
                config.concurrent_per_worker,
            )
            .await?;
            (Some(Arc::new(pool)), Some(bridge_task))
        } else {
            (None, None)
        };

    let python_remote_pool =
        waymark_worker_remote_pool::RemoteWorkerPool::new(python_process_pool.clone());
    let javascript_remote_pool = javascript_process_pool
        .as_ref()
        .map(|pool| waymark_worker_remote_pool::RemoteWorkerPool::new(pool.clone()));
    let runtime_pool = Arc::new(waymark_worker_remote_pool::RuntimeWorkerPool::new(
        python_remote_pool,
        javascript_remote_pool,
    ));

    // Start the webapp server.
    let webapp_backend = Arc::new(backend.clone());
    let maybe_webapp_handle = waymark_webapp_bringup::start(
        config.webapp.clone(),
        webapp_backend,
        shutdown_token.clone().cancelled_owned(),
    )
    .await?;

    let active_instance_gauge = Arc::new(AtomicUsize::new(0));

    // Start status reporting.
    let pool_id = Uuid::new_v4();
    let status_reporter_handle = tokio::spawn(waymark_worker_status_reporter::run(
        pool_id,
        backend.clone(),
        runtime_pool.clone(),
        active_instance_gauge.clone(),
        config.profile_interval,
        shutdown_token.clone().cancelled_owned(),
    ));

    let shutdown_handle = tokio::spawn({
        let shutdown_token = shutdown_token.clone();
        async move {
            if let Err(err) = wait_for_shutdown().await {
                error!(error = %err, "shutdown signal listener failed");
                return;
            }
            info!("shutdown signal received");
            shutdown_token.cancel();
        }
    });

    // Start the execution subsystem (workload pinning + execution driver).
    let bringup_config = waymark_execution_bringup::Config {
        node_id: Uuid::new_v4(),
        action_effect_reconciler_lock_ttl: config.action_effect_reconciler_lock_ttl,
        action_effect_reconciler_lock_heartbeat: config.action_effect_reconciler_lock_heartbeat,
        max_pinned: config.max_concurrent_instances,
        pinning_ttl: config.lock_ttl,
        pinning_heartbeat: config.lock_heartbeat,
        vm_retention: config.vm_retention,
        vm_sweep_interval: config.vm_sweep_interval,
        executable_retention: config.executable_retention,
        executable_sweep_interval: config.executable_sweep_interval,
    };
    let execution_handles = waymark_execution_bringup::start(
        bringup_config,
        Arc::new(backend.clone()),
        runtime_pool.clone(),
        shutdown_token.child_token(),
        force_shutdown_token.child_token(),
    )
    .await?;

    let _ = shutdown_handle.await;
    let _ = tokio::time::timeout(Duration::from_secs(5), execution_handles.pinning_manager).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), execution_handles.execution_driver).await;
    let _ =
        tokio::time::timeout(Duration::from_secs(2), execution_handles.executable_sweeper).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), execution_handles.vm_sweeper).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.durable_action_completions_writer,
    )
    .await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.durable_action_completions_poller,
    )
    .await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.durable_action_completions_acker,
    )
    .await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.action_effect_reconciler_lock_renewal,
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(5), python_bridge_task).await;
    if let Some(javascript_bridge_task) = javascript_bridge_task {
        let _ = tokio::time::timeout(Duration::from_secs(5), javascript_bridge_task).await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), status_reporter_handle).await;

    drop(runtime_pool);
    if let Err(err) = python_process_pool.shutdown_arc().await {
        warn!(error = %err, "Python worker pool shutdown failed");
    }
    if let Some(javascript_process_pool) = javascript_process_pool
        && let Err(err) = javascript_process_pool.shutdown_arc().await
    {
        warn!(error = %err, "JavaScript worker pool shutdown failed");
    }

    if let Some(webapp_handle) = maybe_webapp_handle {
        // Wait for graceful termination.
        webapp_handle.await.unwrap();
    }

    info!("shutdown complete");
    Ok(())
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
