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
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string (required)
//! - WAYMARK_WORKER_GRPC_ADDR: gRPC server for worker connections (default: 127.0.0.1:24118)
//! - WAYMARK_USER_MODULE: Python module(s) to preload (comma-separated)
//! - WAYMARK_WORKER_COUNT: Number of workers (default: num_cpus)
//! - WAYMARK_CONCURRENT_PER_WORKER: Max concurrent actions per worker (default: 10)
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
use prost::Message;
use sqlx::{PgPool, Row};
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use uuid::Uuid;
use waymark::backends::PostgresBackend;
use waymark::config::WorkerConfig;
use waymark::db;
use waymark::messages::ast as ir;
use waymark::scheduler::{DagResolver, WorkflowDag};
use waymark::waymark_core::runloop::{RunLoopSupervisorConfig, runloop_supervisor};
use waymark::{PythonWorkerConfig, RemoteWorkerPool, WebappServer, spawn_status_reporter};
use waymark_dag::convert_to_dag;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "waymark=info,start_workers=info".into()),
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
        lock_ttl_ms = config.lock_ttl.as_millis(),
        lock_heartbeat_ms = config.lock_heartbeat.as_millis(),
        evict_sleep_threshold_ms = config.evict_sleep_threshold.as_millis(),
        expired_lock_reclaimer_interval_ms = config.expired_lock_reclaimer_interval.as_millis(),
        expired_lock_reclaimer_batch_size = config.expired_lock_reclaimer_batch_size,
        garbage_collector_interval_ms = config.garbage_collector.interval.as_millis(),
        garbage_collector_batch_size = config.garbage_collector.batch_size,
        garbage_collector_retention_secs = config.garbage_collector.retention.as_secs(),
        max_action_lifecycle = ?config.max_action_lifecycle,
        "starting worker infrastructure"
    );

    // Wire shutdown coordination.
    let shutdown_token = tokio_util::sync::CancellationToken::new();

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
    let scheduler_handle = {
        let shutdown = shutdown_token.clone().cancelled_owned();
        let task = waymark::SchedulerTask {
            backend: backend.clone(),
            config: config.scheduler.clone(),
            dag_resolver,
        };
        tokio::spawn(task.run(shutdown))
    };
    info!(
        poll_interval_ms = config.scheduler.poll_interval.as_millis(),
        batch_size = config.scheduler.batch_size,
        "scheduler task started"
    );

    let garbage_collector_handle = {
        let shutdown = shutdown_token.clone().cancelled_owned();
        let task = waymark::GarbageCollectorTask {
            backend: backend.clone(),
            config: config.garbage_collector.clone(),
        };
        tokio::spawn(task.run(shutdown))
    };
    info!(
        interval_ms = config.garbage_collector.interval.as_millis(),
        batch_size = config.garbage_collector.batch_size,
        retention_secs = config.garbage_collector.retention.as_secs(),
        "garbage collector task started"
    );

    let active_instance_gauge = Arc::new(AtomicUsize::new(0));

    // Start status reporting.
    let pool_id = Uuid::new_v4();
    let status_reporter_handle = spawn_status_reporter(
        pool_id,
        backend.clone(),
        remote_pool.clone(),
        active_instance_gauge.clone(),
        config.profile_interval,
        shutdown_token.clone().cancelled_owned(),
    );
    let expired_lock_reclaimer_handle = spawn_expired_lock_reclaimer(
        backend.clone(),
        config.expired_lock_reclaimer_interval,
        config.expired_lock_reclaimer_batch_size,
        shutdown_token.clone().cancelled_owned(),
    );

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

    // Run the runloop supervisor until shutdown.
    let lock_uuid = Uuid::new_v4();
    runloop_supervisor(
        backend.clone(),
        remote_pool.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: config.max_concurrent_instances,
            executor_shards: config.executor_shards,
            instance_done_batch_size: config.instance_done_batch_size,
            poll_interval: config.poll_interval,
            persistence_interval: config.persistence_interval,
            lock_uuid,
            lock_ttl: config.lock_ttl,
            lock_heartbeat: config.lock_heartbeat,
            evict_sleep_threshold: config.evict_sleep_threshold,
            skip_sleep: false,
            active_instance_gauge: Some(active_instance_gauge),
        },
        shutdown_token,
    )
    .await;

    let _ = shutdown_handle.await;
    let _ = tokio::time::timeout(Duration::from_secs(5), scheduler_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), garbage_collector_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), status_reporter_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), expired_lock_reclaimer_handle).await;

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
                    SELECT id, program_proto
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

                let version_id: Uuid = row.get("id");
                let payload: Vec<u8> = row.get("program_proto");
                let program = ir::Program::decode(&payload[..]).ok()?;
                let dag = convert_to_dag(&program).ok()?;
                Some(WorkflowDag {
                    version_id,
                    dag: Arc::new(dag),
                })
            })
        })
    })
}

fn spawn_expired_lock_reclaimer(
    backend: PostgresBackend,
    interval: Duration,
    batch_size: usize,
    shutdown: tokio_util::sync::WaitForCancellationFutureOwned,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        info!(
            interval_ms = interval.as_millis(),
            batch_size, "expired lock reclaimer started"
        );
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let mut reclaimed_total = 0usize;
                    loop {
                        match backend.reclaim_expired_instance_locks(batch_size).await {
                            Ok(reclaimed) => {
                                reclaimed_total += reclaimed;
                                if reclaimed < batch_size {
                                    break;
                                }
                            }
                            Err(err) => {
                                warn!(error = %err, "failed to reclaim expired instance locks");
                                break;
                            }
                        }
                    }
                    if reclaimed_total > 0 {
                        warn!(
                            reclaimed_total,
                            "reclaimed expired instance locks"
                        );
                    }
                }
                _ = &mut shutdown => {
                    info!("expired lock reclaimer shutting down");
                    break;
                }
            }
        }
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
