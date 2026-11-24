//! Waymark Bridge - gRPC server for workflow registration and singleton discovery.
//!
//! This binary starts the Waymark bridge server with:
//! - gRPC WorkflowService for workflow registration
//! - gRPC health check for singleton discovery
//!
//! It never migrates the database nor the observability store: it refuses
//! to start unless each one's schema matches its migrations exactly, and
//! while a migration of either is in progress. It keeps a shared
//! migrations lock on each for as long as it runs, and shuts down when it
//! cannot renew one.
//!
//! It is a node of its own: it mints a node id at every boot and, with a
//! database, emits what it observes — the executables registered through
//! it — under that node id to the observability store. It never migrates
//! the observability store either: it refuses to start unless the store's
//! schema matches its migrations exactly.
//!
//! It shuts down gracefully on Ctrl+C or a termination request.
//!
//! Configuration is via environment variables:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string for every database that has no URL of its own (required unless in-memory, or each has one)
//! - WAYMARK_MAIN_DATABASE_URL: DSN for the main database (default: WAYMARK_DATABASE_URL)
//! - WAYMARK_DATABASE_MAX_CONNECTIONS: connection cap for the database pool (default: 10)
//! - WAYMARK_DATABASE_STATEMENT_TIMEOUT_MS: statement timeout on the database pool, `0` for none (default: 60000)
//! - WAYMARK_DATABASE_ACQUIRE_TIMEOUT_MS: how long taking a connection from the database pool may wait, `0` refuses to boot (default: 30000)
//! - WAYMARK_MIGRATIONS_LOCK_TTL_MS / WAYMARK_MIGRATIONS_LOCK_HEARTBEAT_MS / WAYMARK_MIGRATIONS_LOCK_FENCING_MARGIN_MS: the database's migrations lock: how long it lasts after a renewal, how often it is renewed, and how early before its expiry the bridge shuts down when it could not renew it (defaults: 15000 / 5000 / 1000)
//! - WAYMARK_MIGRATIONS_LOCK_STATEMENT_TIMEOUT_MS: statement timeout on the connection the database's migrations lock is renewed through, `0` for none (default: 3000)
//! - WAYMARK_OBSERVABILITY_DATABASE_URL: DSN for the observability store (default: WAYMARK_DATABASE_URL)
//! - WAYMARK_OBSERVABILITY_POSTGRES_MAX_CONNECTIONS: Connection cap for the observability write pool (default: 4)
//! - WAYMARK_OBSERVABILITY_POSTGRES_STATEMENT_TIMEOUT_MS: Statement timeout on the observability write pool, `0` for none (default: 30000)
//! - WAYMARK_OBSERVABILITY_POSTGRES_ACQUIRE_TIMEOUT_MS: How long taking a connection from the observability write pool may wait, `0` refuses to boot (default: 30000)
//! - WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_TTL_MS / WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_HEARTBEAT_MS / WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_FENCING_MARGIN_MS: the observability store's migrations lock: how long it lasts after a renewal, how often it is renewed, and how early before its expiry the bridge shuts down when it could not renew it (defaults: 15000 / 5000 / 1000)
//! - WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_STATEMENT_TIMEOUT_MS: statement timeout on the connection the observability store's migrations lock is renewed through, `0` for none (default: 3000)
//! - WAYMARK_OBSERVABILITY_EVENTS_SINK_BUFFERS / WAYMARK_OBSERVABILITY_EVENTS_SINK_MAX_BATCH / WAYMARK_OBSERVABILITY_EVENTS_SINK_MAX_DELAY_MS / WAYMARK_OBSERVABILITY_EVENTS_SINK_FLUSHERS: Observability-events write batching (defaults: 3 / 256 / 1000 / 1)
//! - WAYMARK_OBSERVABILITY_EVENTS_RETENTION_MS / WAYMARK_OBSERVABILITY_EVENTS_RETENTION_SWEEP_INTERVAL_MS / WAYMARK_OBSERVABILITY_EVENTS_RECORD_SNAPSHOT_PERSISTED: read and validated at boot as on the node, with no effect on the bridge
//! - WAYMARK_BRIDGE_GRPC_ADDR: gRPC server bind address (default: 127.0.0.1:24117)
//! - WAYMARK_BRIDGE_IN_MEMORY: enable in-memory execution mode for streaming workflows, with no database and no observability

mod bridge_service;
mod workflow_store;

use self::bridge_service::*;
use self::workflow_store::*;

use std::sync::Arc;

use color_eyre::eyre::WrapErr as _;
use tracing::info;
use waymark_managed_spawner_supervised::SupervisorExt as _;

use waymark_proto::messages as proto;

const DEFAULT_GRPC_ADDR: &str = "127.0.0.1:24117";

struct PermissiveBool(pub bool);

impl core::str::FromStr for PermissiveBool {
    type Err = core::convert::Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let lowered = s.trim().to_ascii_lowercase();
        let val = !lowered.is_empty() && lowered != "0" && lowered != "false" && lowered != "no";
        Ok(Self(val))
    }
}

#[tokio::main]
async fn main()
-> Result<waymark_fn_main_lifecycle::CleanShutdown<()>, waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    // The OS's shutdown requests are held from here on, so one landing at
    // any point of the startup is latched for the listener instead of
    // ending the process on the spot, and has no other effect until the
    // listener runs.
    let shutdown_requests = waymark_fn_main_lifecycle_ctrl_c::install()?;

    let grpc_addr: std::net::SocketAddr =
        envfury::or_parse("WAYMARK_BRIDGE_GRPC_ADDR", DEFAULT_GRPC_ADDR)?;
    let PermissiveBool(in_memory) = envfury::or_parse("WAYMARK_BRIDGE_IN_MEMORY", "false")?;

    let databases = if in_memory {
        None
    } else {
        let database = waymark_backend_postgres_config::ReadWriteOnly::from_env(
            waymark_backend_postgres_config::DefaultsFor::Bridge,
        )?;
        let observability_store =
            waymark_observability_store_config::WriteOnly::from_env_url_with_default(
                &database.read_write.url,
            )?;
        let observability_events =
            waymark_observability_events_config::ObservabilityEventsConfig::from_env()?;

        Some((database, observability_store, observability_events))
    };

    // Mint this boot's node identity, which the observability-events
    // emitter is brought up under when there is a database.
    let node_id = waymark_ids::NodeId::new_uuid_v4();

    // Wire shutdown coordination. The signal listener cancels the
    // stop-startup-then-shutdown token; the lifecycle cancels the shutdown
    // token after it, or when the startup fails, and the supervisor cancels
    // it when a task ends early. The force token is deliberately never
    // cancelled: this process is graceful only, and a drain that never
    // finishes is ended by the orchestrator's kill, not by anything in here.
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let force_shutdown_token = tokio_util::sync::CancellationToken::new();

    let supervisor = waymark_managed_spawner_supervised::supervisor::start(shutdown_token.clone());

    // The shared migrations locks the startup takes are handed over here,
    // and kept outside the supervisor: its tasks stop on the shutdown
    // token, and the locks have to outlast their drain.
    let (migrations_locks_handover, migrations_locks_keeping) =
        waymark_postgres_bringup::shared_migrations_locks_keeping();

    let params = waymark_fn_main_lifecycle::Params {
        supervisor,
        stop_startup_then_shutdown_token: stop_startup_then_shutdown_token.clone(),
        shutdown_token: shutdown_token.clone(),
    };
    let lifecycle = waymark_fn_main_lifecycle::run(params, async |supervisor, checkpoint| {
        let mut supervisor = supervisor.spawner(waymark_fn_main_common::ErrorConverter);

        // Held by the startup alone, so it is dropped when the startup
        // returns: no lock is handed over after that.
        let migrations_locks_handover = migrations_locks_handover;

        // The shutdown signal listener: on a request it cancels the
        // stop-startup-then-shutdown token, then waits for the shutdown
        // token the lifecycle cancels in turn, so its end is always after
        // the shutdown request.
        supervisor.spawn(
            "shutdown signal listener",
            waymark_fn_main_lifecycle_ctrl_c::listen(
                shutdown_requests,
                stop_startup_then_shutdown_token.clone(),
                shutdown_token.clone(),
            ),
        );

        let store = match databases {
            None => None,
            Some((database, observability_store, observability_events)) => {
                // The bridge migrates nothing: it takes a shared migrations
                // lock, checks under it that the database is migrated
                // exactly to this build's migrations, and hands the lock
                // over to be kept for as long as it runs on the database.
                let main_database_assertions = waymark_backend_postgres_migrations::assertions();
                let main_database_migrations =
                    waymark_migrations_engine::validate_migrations_array(
                        waymark_backend_postgres_migrations::migrations(),
                    )?;
                let held_main_database_migrations_lock =
                    waymark_postgres_bringup::take_shared_migrations_lock_and_check::<
                        waymark_backend_postgres_migrations::Tables,
                        _,
                        _,
                        _,
                    >(
                        waymark_postgres_bringup::take_shared_migrations_lock_and_check::Params {
                            lock_url: &database.read_write.url,
                            migrations_lock: &database.migrations_lock,
                            schema: None,
                            holder_id:
                                waymark_migrations_engine_backend::migrations_lock::HolderId::from_raw(
                                    node_id.into(),
                                ),
                            assertions: &main_database_assertions,
                            migrations: &main_database_migrations,
                        },
                    )
                    .await?;
                migrations_locks_handover.keep(held_main_database_migrations_lock.keeper)?;
                checkpoint()?;

                let pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
                    config: &database.read_write,
                    transactions: waymark_postgres_bringup::Transactions::Unrestricted,
                    schema: None,
                })
                .await?;
                checkpoint()?;

                let observability_events_emitter =
                    waymark_observability_bringup::start_emitter_only(
                        &mut supervisor,
                        &observability_store,
                        observability_events.lossy_batcher_policy,
                        node_id,
                        &migrations_locks_handover,
                        force_shutdown_token.child_token(),
                    )
                    .await?;
                checkpoint()?;

                // The store holds the emitter: the lossy batcher behind it,
                // which stops on the emitter's last handle, runs as long as
                // the service does.
                let workflow_store =
                    WorkflowStore::new(pool, Arc::new(observability_events_emitter));

                Some(Arc::new(workflow_store))
            }
        };

        // Bound here rather than in the server task, so an address in use
        // fails the startup, with the address, before anything reports
        // serving.
        let listener = tokio::net::TcpListener::bind(grpc_addr)
            .await
            .wrap_err_with(|| format!("bind the gRPC server to {grpc_addr}"))?;

        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        let service = BridgeService {
            store,
            shutdown_token: shutdown_token.child_token(),
        };
        health_reporter
            .set_serving::<proto::workflow_service_server::WorkflowServiceServer<BridgeService>>()
            .await;

        info!(%grpc_addr, in_memory, %node_id, "waymark bridge starting");

        let workflow_service = proto::workflow_service_server::WorkflowServiceServer::new(service)
            .max_decoding_message_size(waymark_proto::GRPC_MAX_MESSAGE_SIZE_BYTES)
            .max_encoding_message_size(waymark_proto::GRPC_MAX_MESSAGE_SIZE_BYTES);

        // The gRPC server: on the shutdown token it stops accepting
        // connections and finishes the requests in flight.
        supervisor.spawn(
            "grpc server",
            tonic::transport::Server::builder()
                .add_service(health_service)
                .add_service(workflow_service)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    shutdown_token.clone().cancelled_owned(),
                ),
        );

        Ok::<(), waymark_fn_main_common::Error>(())
    });

    // The lifecycle and the keeping of the shared migrations locks go on
    // together. The locks are released only once the lifecycle has
    // returned, with every task drained: until then this process may still
    // write to the databases. A lock that is fenced requests the shutdown,
    // and fails the process once the lifecycle has returned.
    let lifecycle_returned = tokio_util::sync::CancellationToken::new();
    let (lifecycle, migrations_locks_kept) = tokio::join!(
        async {
            let lifecycle = lifecycle.await;

            lifecycle_returned.cancel();

            lifecycle
        },
        migrations_locks_keeping.run(
            lifecycle_returned.cancelled(),
            stop_startup_then_shutdown_token.clone(),
        ),
    );

    let clean_shutdown = lifecycle?;

    migrations_locks_kept?;

    Ok(clean_shutdown)
}
