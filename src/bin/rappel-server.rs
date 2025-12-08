//! Main entry point for the rappel server.
//!
//! Starts:
//! - HTTP server for workflow management
//! - gRPC server for action workers
//! - gRPC server for instance workers
//! - Worker pools for both worker types

use std::sync::Arc;

use tokio::net::TcpListener;
use tonic::transport::Server as TonicServer;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::{
    config::Config,
    db::Database,
    messages::{
        action_worker_bridge_server::ActionWorkerBridgeServer,
        instance_worker_bridge_server::InstanceWorkerBridgeServer,
    },
    server::{
        action_bridge::{ActionWorkerBridgeService, ActionWorkerBridgeState},
        http::{create_router, HttpState},
        instance_bridge::{InstanceWorkerBridgeService, InstanceWorkerBridgeState},
    },
    worker::{WorkerPool, WorkerType},
};

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

    // Create shared state with capacity limits from config
    let action_bridge_state = Arc::new(ActionWorkerBridgeState::new(
        config.max_concurrent_per_action_worker,
    ));
    let instance_bridge_state = Arc::new(InstanceWorkerBridgeState::new(
        config.max_concurrent_per_instance_worker,
    ));

    // Create gRPC services
    let action_bridge_service = ActionWorkerBridgeService::new(action_bridge_state.clone(), db.clone());
    let instance_bridge_service = InstanceWorkerBridgeService::new(instance_bridge_state.clone(), db.clone());

    // Create HTTP router
    let http_state = HttpState { db: db.clone() };
    let http_router = create_router(http_state);

    // Create worker pools
    let action_worker_pool = Arc::new(WorkerPool::new(
        WorkerType::Action,
        config.action_grpc_addr.to_string(),
        config.user_modules.clone(),
        config.action_worker_count,
    ));

    let instance_worker_pool = Arc::new(WorkerPool::new(
        WorkerType::Instance,
        config.instance_grpc_addr.to_string(),
        config.user_modules.clone(),
        config.instance_worker_count,
    ));

    // Spawn HTTP server
    let http_addr = config.http_addr;
    let http_handle = tokio::spawn(async move {
        info!(%http_addr, "Starting HTTP server");
        let listener = TcpListener::bind(http_addr).await?;
        axum::serve(listener, http_router).await?;
        Ok::<_, anyhow::Error>(())
    });

    // Spawn action worker gRPC server
    let action_grpc_addr = config.action_grpc_addr;
    let action_grpc_handle = tokio::spawn(async move {
        info!(%action_grpc_addr, "Starting action worker gRPC server");
        TonicServer::builder()
            .add_service(ActionWorkerBridgeServer::new(action_bridge_service))
            .serve(action_grpc_addr)
            .await?;
        Ok::<_, anyhow::Error>(())
    });

    // Spawn instance worker gRPC server
    let instance_grpc_addr = config.instance_grpc_addr;
    let instance_grpc_handle = tokio::spawn(async move {
        info!(%instance_grpc_addr, "Starting instance worker gRPC server");
        TonicServer::builder()
            .add_service(InstanceWorkerBridgeServer::new(instance_bridge_service))
            .serve(instance_grpc_addr)
            .await?;
        Ok::<_, anyhow::Error>(())
    });

    // Start worker pools
    info!("Starting worker pools");
    action_worker_pool.start().await?;
    instance_worker_pool.start().await?;

    // Spawn worker maintenance task
    let action_pool = action_worker_pool.clone();
    let instance_pool = instance_worker_pool.clone();
    let maintenance_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            if let Err(e) = action_pool.maintain().await {
                error!(error = %e, "Failed to maintain action worker pool");
            }
            if let Err(e) = instance_pool.maintain().await {
                error!(error = %e, "Failed to maintain instance worker pool");
            }
        }
    });

    // Spawn timeout recovery task
    let recovery_db = db.clone();
    let recovery_handle = tokio::spawn(async move {
        loop {
            // Check for timeouts every 10 seconds
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

            // Recover timed-out actions (reset to queued for retry)
            match recovery_db.recover_timed_out_actions().await {
                Ok(count) if count > 0 => {
                    info!(count, "Recovered timed-out actions");
                }
                Err(e) => {
                    error!(error = %e, "Failed to recover timed-out actions");
                }
                _ => {}
            }

            // Fail actions that exceeded max attempts
            match recovery_db.fail_exhausted_actions().await {
                Ok(count) if count > 0 => {
                    info!(count, "Failed exhausted actions");
                }
                Err(e) => {
                    error!(error = %e, "Failed to mark exhausted actions");
                }
                _ => {}
            }

            // Recover timed-out instances
            match recovery_db.recover_timed_out_instances().await {
                Ok(count) if count > 0 => {
                    info!(count, "Recovered timed-out instances");
                }
                Err(e) => {
                    error!(error = %e, "Failed to recover timed-out instances");
                }
                _ => {}
            }
        }
    });

    // Spawn action dispatcher task
    // Claims queued actions from DB (limited by available slots) and dispatches to workers
    let action_dispatcher_db = db.clone();
    let action_dispatcher_state = action_bridge_state.clone();
    let action_dispatcher_handle = tokio::spawn(async move {
        loop {
            // Poll frequently for low latency
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            // Only claim as many as we have capacity for
            let available = action_dispatcher_state.available_slots().await;
            if available == 0 {
                continue;
            }

            // Claim actions from DB
            match action_dispatcher_db
                .claim_queued_actions(available as i32)
                .await
            {
                Ok(actions) => {
                    for action in actions {
                        let dispatch = rappel::messages::ActionDispatch {
                            action_id: action.id.to_string(),
                            instance_id: action.instance_id.to_string(),
                            sequence: action.sequence as u32,
                            action_name: action.action_name,
                            module_name: action.module_name,
                            kwargs: None, // TODO: Convert from JSON
                            timeout_seconds: Some(action.timeout_seconds as u32),
                            max_retries: Some(action.max_attempts as u32),
                            attempt_number: Some(action.attempt_count as u32),
                            dispatch_token: action.dispatch_token.map(|t| t.to_string()),
                        };

                        if let Err(e) = action_dispatcher_state.dispatch_action(dispatch).await {
                            error!(error = %e, "Failed to dispatch action");
                            // Action will timeout and be recovered
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to claim queued actions");
                }
            }
        }
    });

    // Spawn instance dispatcher task
    // Claims ready instances from DB (limited by available slots) and dispatches to workers
    let instance_dispatcher_db = db.clone();
    let instance_dispatcher_state = instance_bridge_state.clone();
    let instance_dispatcher_handle = tokio::spawn(async move {
        loop {
            // Poll frequently for low latency
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            // Only claim as many as we have capacity for
            let available = instance_dispatcher_state.available_slots().await;
            if available == 0 {
                continue;
            }

            // Claim ready instances from DB
            match instance_dispatcher_db
                .claim_ready_instances(available as i32)
                .await
            {
                Ok(instances) => {
                    for instance in instances {
                        // Fetch completed actions for replay
                        let completed_actions = match instance_dispatcher_db
                            .get_completed_actions(instance.id)
                            .await
                        {
                            Ok(actions) => actions,
                            Err(e) => {
                                error!(error = %e, "Failed to get completed actions");
                                continue;
                            }
                        };

                        let dispatch = rappel::messages::InstanceDispatch {
                            instance_id: instance.id.to_string(),
                            workflow_name: instance.workflow_name,
                            module_name: instance.module_name,
                            actions_until_index: instance.actions_until_index as u32,
                            initial_args: None, // TODO: Convert from JSON
                            completed_actions: completed_actions
                                .into_iter()
                                .map(|a| rappel::messages::ActionResult {
                                    action_id: a.id.to_string(),
                                    success: a.status == rappel::db::ActionStatus::Completed,
                                    payload: None, // TODO: Convert from JSON
                                    worker_start_ns: 0,
                                    worker_end_ns: 0,
                                    dispatch_token: None,
                                    error_type: None,
                                    error_message: a.error_message,
                                })
                                .collect(),
                            scheduled_at_ms: instance
                                .scheduled_at
                                .map(|t| t.timestamp_millis())
                                .unwrap_or(0),
                            dispatch_token: instance.dispatch_token.map(|t| t.to_string()),
                        };

                        if let Err(e) = instance_dispatcher_state.dispatch_instance(dispatch).await
                        {
                            error!(error = %e, "Failed to dispatch instance");
                            // Instance will timeout and be recovered
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to claim ready instances");
                }
            }
        }
    });

    info!("Rappel server started");

    // Wait for any task to fail
    tokio::select! {
        result = http_handle => {
            error!(?result, "HTTP server exited");
        }
        result = action_grpc_handle => {
            error!(?result, "Action gRPC server exited");
        }
        result = instance_grpc_handle => {
            error!(?result, "Instance gRPC server exited");
        }
        _ = maintenance_handle => {
            error!("Maintenance task exited");
        }
        _ = recovery_handle => {
            error!("Recovery task exited");
        }
        _ = action_dispatcher_handle => {
            error!("Action dispatcher task exited");
        }
        _ = instance_dispatcher_handle => {
            error!("Instance dispatcher task exited");
        }
    }

    // Cleanup
    action_worker_pool.stop().await;
    instance_worker_pool.stop().await;

    Ok(())
}
