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

use std::sync::Arc;

use tracing::{error, info};

use waymark_backend_postgres::PostgresBackend;
use waymark_config::WorkerConfig;

/// The supervisor of this process's tasks: their errors differ per
/// subsystem, so they are supervised erased.
type Supervisor = waymark_task_supervisor::Supervisor<waymark_task_supervisor::BoxedError>;

/// The process exit when a task ended before the shutdown was requested.
#[derive(Debug, thiserror::Error)]
#[error("shutdown was failure-driven: a task ended before it was requested")]
struct FailureDrivenShutdown;

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    // The OS's shutdown requests are held from here on, so one landing at
    // any point of the startup is latched for the listener.
    let ctrl_c = waymark_os_shutdown_requests::ctrl_c::install()?;
    let termination = waymark_os_shutdown_requests::termination::install()?;

    let metrics_addr: std::net::SocketAddr = envfury::or_parse("METRICS_ADDR", "0.0.0.0:9118")?;
    let essential_metrics_sampling_handle = waymark_metrics_bringup::start(metrics_addr)?;

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
    )
    .set(1);

    // Wire shutdown coordination. The graceful token is the only one ever
    // cancelled: by the signal listener on a signal, or by the supervisor
    // when a task ends early. The force token is handed to the execution
    // subsystem and deliberately never cancelled: this process is graceful
    // only, and a drain that never finishes is ended by the orchestrator's
    // kill, not by anything in here.
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();

    let mut supervisor: Supervisor = waymark_task_supervisor::start(shutdown_token.clone());

    // Bring everything up under the supervisor, handing each subsystem's
    // tasks over as soon as they exist. A failure part-way leaves the tasks
    // already up supervised; they are shut down and drained like on any
    // other failure, and the boot error is what main returns.
    let started: Result<(), waymark_fn_main_common::Error> = async {
        // The shutdown signal listener: on a request it requests the
        // shutdown, so its end is always after the request.
        supervisor.spawn(
            "shutdown signal listener",
            shutdown_signal_listener(ctrl_c, termination, shutdown_token.clone()),
        );

        // Initialize the database and backend.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.database_max_connections.get())
            .connect(config.database_url.expose_secret())
            .await?;
        waymark_backend_postgres_migrations::run(&pool).await?;
        let backend = PostgresBackend::new(pool);

        // Start the observability pipelines.
        let (
            observability_handles,
            observability_api_router,
            observability_events_emitter,
            vm_driver_hooks_policy,
        ) = waymark_observability_bringup::start(
            config.observability.clone(),
            node_id,
            essential_metrics_sampling_handle,
            shutdown_token.child_token(),
        )
        .await?;

        supervisor.track(
            "essential metrics sampler",
            observability_handles.essential_metrics.sampler,
        );
        supervisor.track(
            "essential metrics batcher",
            observability_handles.essential_metrics.batcher,
        );
        supervisor.track(
            "essential metrics retention",
            observability_handles.essential_metrics.retention,
        );
        supervisor.track(
            "observability events batcher",
            observability_handles.observability_events.batcher,
        );
        supervisor.track(
            "observability events retention",
            observability_handles.observability_events.retention,
        );

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

        supervisor.track("worker bridge server", bridge_task);

        let process_pool = Arc::new(process_pool);

        let remote_pool = Arc::new(waymark_worker_remote_pool::RemoteWorkerPool::new(
            process_pool.clone(),
        ));

        // Compose everything the HTTP server serves.
        let http_api_routes = aide::axum::ApiRouter::new().merge(observability_api_router);
        let http_routes = axum::Router::new()
            .merge(waymark_http_healthz::router())
            .merge(waymark_http_api::router("/api", http_api_routes));

        // Start the HTTP server.
        if config.http.enabled {
            let http_task = waymark_http_bringup::start(
                config.http.addr,
                http_routes,
                shutdown_token.clone().cancelled_owned(),
            )
            .await?;

            supervisor.track("http server", http_task);
        } else {
            info!("http server disabled (set WAYMARK_HTTP_ENABLED=true to enable)");
        }

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
            action_effect_reconciler_lock_batch_delay: config
                .action_effect_reconciler_lock_batch_delay,
            sleep_poll_interval: config.sleep_poll_interval,
            vm_retention: config.vm_retention,
            vm_sweep_interval: config.vm_sweep_interval,
            executable_retention: config.executable_retention,
            executable_sweep_interval: config.executable_sweep_interval,
        };

        let execution_handles = waymark_execution_bringup::start(
            bringup_config,
            Arc::new(backend.clone()),
            remote_pool,
            Some(waymark_execution_bringup::ObservabilityEvents {
                emitter: Arc::new(observability_events_emitter),
                vm_driver_hooks_policy,
            }),
            shutdown_token.child_token(),
            force_shutdown_token.child_token(),
        )
        .await?;

        let waymark_execution_bringup::Handles {
            pinning_manager,
            execution_driver,
            executable_sweeper,
            vm_sweeper,
            durable_action_completions_writer,
            durable_action_completions_poller,
            durable_action_completions_acker,
            durable_sleeps_poller,
            durable_sleeps_acker,
            action_effect_reconciler_lock_renewal,
            snapshot_batcher,
            action_effect_reconciler_request_batcher,
            workflow_completion_batcher,
            action_effect_reconciler_lock_batcher,
        } = execution_handles;

        let execution_tasks = [
            ("workload pinning manager", pinning_manager),
            ("execution driver", execution_driver),
            ("executable sweeper", executable_sweeper),
            ("vm runtimes sweeper", vm_sweeper),
            (
                "durable action completions writer",
                durable_action_completions_writer,
            ),
            (
                "durable action completions poller",
                durable_action_completions_poller,
            ),
            (
                "durable action completions acker",
                durable_action_completions_acker,
            ),
            ("durable sleeps poller", durable_sleeps_poller),
            ("durable sleeps acker", durable_sleeps_acker),
            (
                "action effect reconciler lock renewal",
                action_effect_reconciler_lock_renewal,
            ),
            ("snapshot batcher", snapshot_batcher),
            (
                "action effect reconciler request batcher",
                action_effect_reconciler_request_batcher,
            ),
            ("workflow completion batcher", workflow_completion_batcher),
            (
                "action effect reconciler lock batcher",
                action_effect_reconciler_lock_batcher,
            ),
        ];

        // The worker pool is shut down after every execution task has
        // ended: those hold the pool through the remote pool, and the pool
        // shuts down only once it is the last holder. Each execution task
        // holds one permit for as long as it runs; the shutdown task takes
        // them all back. The workers' streams then end, which is what lets
        // the bridge server's own graceful shutdown complete.
        let execution_task_count = u32::try_from(execution_tasks.len())
            .expect("the execution bringup hands out a handful of tasks");
        let execution_task_permits = Arc::new(tokio::sync::Semaphore::new(execution_tasks.len()));

        supervisor.spawn("worker pool shutdown", {
            let shutdown_token = shutdown_token.clone();
            let execution_task_permits = execution_task_permits.clone();
            async move {
                shutdown_token.cancelled().await;

                let _all_execution_tasks_ended = execution_task_permits
                    .acquire_many_owned(execution_task_count)
                    .await?;

                process_pool.shutdown_arc().await?;

                Ok::<(), waymark_task_supervisor::BoxedError>(())
            }
        });

        for (name, task) in execution_tasks {
            let permit = execution_task_permits
                .clone()
                .try_acquire_owned()
                .expect("one permit per execution task, none taken twice");

            supervisor.spawn(name, async move {
                let _held_while_running = permit;

                task.await
            });
        }

        // Start the scheduler subsystem (due-schedule polling + spawning).
        let scheduler_task = waymark_scheduler_bringup::start(
            waymark_scheduler_bringup::Config {
                poll_interval: config.scheduler_poll_interval,
                max_items: config.scheduler_batch_max,
            },
            Arc::new(backend.clone()),
            shutdown_token.child_token(),
        );

        supervisor.track("scheduler", scheduler_task);

        Ok(())
    }
    .await;

    if let Err(error) = &started {
        error!(error = %error, "startup failed; shutting down");
        shutdown_token.cancel();
    }

    let report = supervisor.drain().await;

    if report.any_before_shutdown() {
        error!(%report, "shutdown complete");
    } else {
        info!(%report, "shutdown complete");
    }

    started?;

    if report.any_before_shutdown() {
        return Err(FailureDrivenShutdown.into());
    }

    Ok(())
}

/// Wait for the OS's first shutdown request of either kind, then request
/// the shutdown.
async fn shutdown_signal_listener(
    mut ctrl_c: waymark_os_shutdown_requests::ctrl_c::Receiver,
    mut termination: waymark_os_shutdown_requests::termination::Receiver,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), std::convert::Infallible> {
    tokio::select! {
        () = ctrl_c.recv() => {
            info!("Ctrl+C received");
        }
        () = termination.recv() => {
            info!("termination requested");
        }
    }
    info!("shutdown signal received");
    shutdown_token.cancel();

    Ok(())
}
