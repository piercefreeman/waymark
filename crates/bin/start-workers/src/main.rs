//! Start Workers - Runs the durable VM execution subsystem with a Python worker pool.
//!
//! This binary starts the worker infrastructure:
//! - Connects to the database
//! - Starts the WorkerBridge gRPC server for worker connections
//! - Spawns a pool of Python workers
//! - Runs the durable VM execution subsystem (workload pinning, VM drivers,
//!   action/sleep reconcilers, completion writers, snapshot/request batchers)
//! - Optionally starts the web dashboard
//!
//! Configuration is via environment variables:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string (required)
//! - WAYMARK_WORKER_GRPC_ADDR: gRPC server for worker connections (default: 127.0.0.1:24118)
//! - WAYMARK_USER_MODULE: Python module(s) to preload (comma-separated)
//! - WAYMARK_WORKER_COUNT: Number of workers (default: num_cpus)
//! - WAYMARK_CONCURRENT_PER_WORKER: Max concurrent actions per worker (default: 10)
//! - WAYMARK_MAX_CONCURRENT_INSTANCES: Max workflow instances held concurrently (default: 500)
//! - WAYMARK_MAX_ACTION_LIFECYCLE: Max actions per worker before recycling
//! - WAYMARK_LOCK_TTL_MS: Workload pinning TTL (default: 15000)
//! - WAYMARK_LOCK_HEARTBEAT_MS: Pinning refresh heartbeat interval (default: 5000)
//! - WAYMARK_PINNING_FENCING_MARGIN_MS: How early a pinning is fenced before its ttl (default: 1000)
//! - WAYMARK_WORKLOAD_POLL_INTERVAL_NS: Min interval between unpinned-workload polls (default: 1000000)
//! - WAYMARK_SNAPSHOT_BATCH_MAX / WAYMARK_SNAPSHOT_BATCH_DELAY_MS: Snapshot write batching
//! - WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_MAX / _DELAY_MS: Request write batching
//! - WAYMARK_WORKFLOW_COMPLETION_BATCH_MAX / _DELAY_MS: Workflow outcome write batching
//! - WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_MAX / _DELAY_MS: Request lock batching
//! - WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_TTL_MS / _HEARTBEAT_MS: Request lock lease timing
//! - WAYMARK_SLEEP_POLL_INTERVAL_MS: Durable sleep poll interval (default: 250)
//! - WAYMARK_SCHEDULER_POLL_INTERVAL_MS: Due-schedule poll interval (default: 1000)
//! - WAYMARK_SCHEDULER_BATCH_MAX: Max due schedules spawned per poll (default: 64)
//! - WAYMARK_VM_RETENTION_MS / WAYMARK_VM_SWEEP_INTERVAL_MS: Cached VM eviction
//! - WAYMARK_EXECUTABLE_RETENTION_MS / WAYMARK_EXECUTABLE_SWEEP_INTERVAL_MS: Cached executable eviction
//! - WAYMARK_HTTP_ENABLED: Serve the HTTP interface (default: false)
//! - WAYMARK_HTTP_ADDR: HTTP server bind address (default: 0.0.0.0:24119)
//! - WAYMARK_RUNNER_PROFILE_INTERVAL_MS: Status reporting interval (default: 5000)

use std::sync::{Arc, atomic::AtomicUsize};
use std::time::Duration;

use sqlx::PgPool;
use tokio::signal;
use tracing::{error, info, warn};
use uuid::Uuid;

use waymark_backend_postgres::PostgresBackend;
use waymark_config::WorkerConfig;

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let metrics_addr: std::net::SocketAddr = envfury::or_parse("METRICS_ADDR", "0.0.0.0:9118")?;
    waymark_prometheus_exporter_bringup::spawn_and_install_recorder(metrics_addr)?;

    let _task_monitor = waymark_tokio_metrics_bringup::bringup(env!("CARGO_BIN_NAME"));

    // Load configuration and announce startup.
    let config = WorkerConfig::from_env()?;

    tracing::debug!(target: "raw-config", ?config, "raw config");

    // Mint this boot's node identity, shared by every subsystem that
    // identifies the node.
    let node_id = waymark_ids::NodeId::new_uuid_v4();

    info!(
        %node_id,
        worker_count = config.worker_count,
        concurrent_per_worker = config.concurrent_per_worker,
        user_modules = ?config.user_modules,
        "starting worker infrastructure"
    );

    metrics::gauge!(
        "waymark_start_workers_up",
        "node_id" => node_id.to_string(),
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

    // Start the worker pool (bridge + python workers).
    let mut worker_config = waymark_worker_python::Config::new();
    if !config.user_modules.is_empty() {
        worker_config = worker_config.with_user_modules(config.user_modules.clone());
    }

    let worker_process_spec_builder = |bridge_server_addr| waymark_worker_python::Spec {
        bridge_server_addr,
        config: worker_config,
    };

    let (process_pool, bridge_task) = waymark_worker_remote_bringup::start(
        shutdown_token.clone(),
        Some(config.worker_grpc_addr),
        worker_process_spec_builder,
        config.worker_count,
        config.max_action_lifecycle,
        config.concurrent_per_worker,
    )
    .await?;

    let process_pool = Arc::new(process_pool);

    // Compose everything the HTTP server serves.
    let http_api_routes = aide::axum::ApiRouter::new();
    let http_routes = axum::Router::new()
        .merge(waymark_http_healthz::router())
        .merge(waymark_http_api::router("/api", http_api_routes));

    // Start the HTTP server.
    let maybe_http_handle = if config.http.enabled {
        let handle = waymark_http_bringup::start(
            config.http.addr,
            http_routes,
            shutdown_token.clone().cancelled_owned(),
        )
        .await?;
        Some(handle)
    } else {
        info!("http server disabled (set WAYMARK_HTTP_ENABLED=true to enable)");
        None
    };

    let active_instance_gauge = Arc::new(AtomicUsize::new(0));

    // Start status reporting.
    let pool_id = Uuid::new_v4();
    let status_reporter_handle = tokio::spawn(waymark_worker_status_reporter::run(
        pool_id,
        backend.clone(),
        process_pool.clone(),
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
        node_id: node_id.into(),
        action_effect_reconciler_lock_ttl: config.action_effect_reconciler_lock_ttl,
        action_effect_reconciler_lock_heartbeat: config.action_effect_reconciler_lock_heartbeat,
        max_pinned: config.max_concurrent_instances,
        pinning_ttl: config.lock_ttl,
        pinning_heartbeat: config.lock_heartbeat,
        pinning_fencing_margin: config.pinning_fencing_margin,
        workload_poll_interval: config.workload_poll_interval,
        snapshot_batch_max: config.snapshot_batch_max,
        snapshot_batch_delay: config.snapshot_batch_delay,
        action_effect_reconciler_request_batch_max: config
            .action_effect_reconciler_request_batch_max,
        action_effect_reconciler_request_batch_delay: config
            .action_effect_reconciler_request_batch_delay,
        workflow_completion_batch_max: config.workflow_completion_batch_max,
        workflow_completion_batch_delay: config.workflow_completion_batch_delay,
        action_effect_reconciler_lock_batch_max: config.action_effect_reconciler_lock_batch_max,
        action_effect_reconciler_lock_batch_delay: config.action_effect_reconciler_lock_batch_delay,
        sleep_poll_interval: config.sleep_poll_interval,
        vm_retention: config.vm_retention,
        vm_sweep_interval: config.vm_sweep_interval,
        executable_retention: config.executable_retention,
        executable_sweep_interval: config.executable_sweep_interval,
    };
    let remote_pool = Arc::new(waymark_worker_remote_pool::RemoteWorkerPool::new(
        process_pool.clone(),
    ));
    let execution_handles = waymark_execution_bringup::start(
        bringup_config,
        Arc::new(backend.clone()),
        remote_pool,
        shutdown_token.child_token(),
        force_shutdown_token.child_token(),
    )
    .await?;

    // Start the scheduler subsystem (due-schedule polling + spawning).
    let scheduler_handle = waymark_scheduler_bringup::start(
        waymark_scheduler_bringup::Config {
            poll_interval: config.scheduler_poll_interval,
            max_items: config.scheduler_batch_max,
        },
        Arc::new(backend.clone()),
        shutdown_token.child_token(),
    );

    let _ = shutdown_handle.await;
    let _ = tokio::time::timeout(Duration::from_secs(5), scheduler_handle).await;
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
    let _ = tokio::time::timeout(Duration::from_secs(5), execution_handles.snapshot_batcher).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.action_effect_reconciler_request_batcher,
    )
    .await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.workflow_completion_batcher,
    )
    .await;
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        execution_handles.action_effect_reconciler_lock_batcher,
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(5), bridge_task).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), status_reporter_handle).await;

    if let Err(err) = process_pool.shutdown_arc().await {
        warn!(error = %err, "worker pool shutdown failed");
    }

    if let Some(http_handle) = maybe_http_handle {
        // Wait for graceful termination.
        let _ = tokio::time::timeout(Duration::from_secs(5), http_handle).await;
    }

    info!("shutdown complete");
    Ok(())
}

async fn wait_for_shutdown() -> Result<(), std::io::Error> {
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
