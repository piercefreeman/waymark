//! Waymark Bridge - gRPC server for workflow registration and singleton discovery.
//!
//! This binary starts the Waymark bridge server with:
//! - gRPC WorkflowService for workflow registration
//! - gRPC health check for singleton discovery
//!
//! It never migrates the database: it refuses to start unless the schema
//! matches its migrations exactly.
//!
//! It shuts down gracefully on Ctrl+C or a termination request.
//!
//! Configuration is via environment variables:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string (required unless in-memory)
//! - WAYMARK_DATABASE_MAX_CONNECTIONS: connection cap for the database pool (default: 10)
//! - WAYMARK_DATABASE_STATEMENT_TIMEOUT_MS: statement timeout on the database pool, `0` for none (default: 60000)
//! - WAYMARK_DATABASE_ACQUIRE_TIMEOUT_MS: how long taking a connection from the database pool may wait, `0` refuses to boot (default: 30000)
//! - WAYMARK_MIGRATIONS_STATEMENT_TIMEOUT_MS: how long the startup waits for a migration in progress, `0` for no limit (default: 600000)
//! - WAYMARK_BRIDGE_GRPC_ADDR: gRPC server bind address (default: 127.0.0.1:24117)
//! - WAYMARK_BRIDGE_IN_MEMORY: enable in-memory execution mode for streaming workflows

mod bridge_service;
mod workflow_store;

use self::bridge_service::*;
use self::workflow_store::*;

use std::sync::Arc;

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

    let database = if in_memory {
        None
    } else {
        Some(waymark_backend_postgres_config::ReadWriteOnly::from_env(
            waymark_backend_postgres_config::DefaultsFor::Bridge,
        )?)
    };

    // Wire shutdown coordination. The signal listener cancels the
    // stop-startup-then-shutdown token; the lifecycle cancels the shutdown
    // token after it, or when the startup fails, and the supervisor cancels
    // it when a task ends early.
    let stop_startup_then_shutdown_token = tokio_util::sync::CancellationToken::new();
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let supervisor = waymark_managed_spawner_supervised::supervisor::start(shutdown_token.clone());

    let params = waymark_fn_main_lifecycle::Params {
        supervisor,
        stop_startup_then_shutdown_token: stop_startup_then_shutdown_token.clone(),
        shutdown_token: shutdown_token.clone(),
    };
    let clean_shutdown = waymark_fn_main_lifecycle::run(params, async |supervisor, checkpoint| {
        let mut supervisor = supervisor.spawner(waymark_fn_main_common::ErrorConverter);

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

        let store = match database {
            None => None,
            Some(database) => {
                // The check waits for a migration in progress, which the
                // pool's statement timeout would cut short.
                let mut migrations_check_connection = waymark_postgres_bringup::connection(
                    waymark_postgres_bringup::ConnectionParams {
                        config: &database.migrations_check,
                        transactions: waymark_postgres_bringup::Transactions::Unrestricted,
                        schema: None,
                    },
                )
                .await?;

                waymark_backend_postgres_migrations::check(&mut migrations_check_connection)
                    .await?;

                <sqlx::PgConnection as sqlx::Connection>::close(migrations_check_connection)
                    .await?;

                let pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
                    config: &database.read_write,
                    transactions: waymark_postgres_bringup::Transactions::Unrestricted,
                    schema: None,
                })
                .await?;

                let workflow_store = WorkflowStore::new(pool);
                Some(Arc::new(workflow_store))
            }
        };
        checkpoint()?;

        // Bound here rather than in the server task, so an address in use
        // fails the startup, with the address, before anything reports
        // serving.
        let listener = tokio::net::TcpListener::bind(grpc_addr).await;
        let listener = color_eyre::eyre::WrapErr::wrap_err_with(listener, || {
            format!("bind the gRPC server to {grpc_addr}")
        })?;

        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        let service = BridgeService {
            store,
            shutdown_token: shutdown_token.child_token(),
        };
        health_reporter
            .set_serving::<proto::workflow_service_server::WorkflowServiceServer<BridgeService>>()
            .await;

        info!(%grpc_addr, in_memory, "waymark bridge starting");

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
    })
    .await?;
    Ok(clean_shutdown)
}
