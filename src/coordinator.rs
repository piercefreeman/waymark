//! Host coordinator for distributed workflow execution.
//!
//! A HostCoordinator represents a single host in the cluster. It:
//! - Runs gRPC servers for action and instance workers
//! - Manages worker pools (spawns Python processes)
//! - Polls the database for work (capacity-gated)
//! - Dispatches work to connected workers
//! - Handles timeout recovery
//!
//! Multiple HostCoordinators can run against the same database to simulate
//! or implement a multi-host cluster.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server as TonicServer;
use tracing::{error, info};

use crate::db::{ActionStatus, Database};
use crate::messages::{
    action_worker_bridge_server::ActionWorkerBridgeServer,
    instance_worker_bridge_server::InstanceWorkerBridgeServer, ActionDispatch, ActionResult,
    InstanceDispatch, PrimitiveWorkflowArgument, WorkflowArgument, WorkflowArgumentValue,
    WorkflowArguments,
};
use crate::server::action_bridge::{ActionWorkerBridgeService, ActionWorkerBridgeState};
use crate::server::http::{create_router, HttpState};
use crate::server::instance_bridge::{InstanceWorkerBridgeService, InstanceWorkerBridgeState};
use crate::worker::{WorkerPool, WorkerType};

/// Convert a JSON value to proto WorkflowArgumentValue.
fn json_to_proto_value(value: &serde_json::Value) -> WorkflowArgumentValue {
    use crate::messages::workflow_argument_value::Kind;

    let kind = match value {
        serde_json::Value::Null => Some(Kind::Primitive(PrimitiveWorkflowArgument {
            kind: Some(crate::messages::primitive_workflow_argument::Kind::NullValue(0)),
        })),
        serde_json::Value::Bool(b) => Some(Kind::Primitive(PrimitiveWorkflowArgument {
            kind: Some(crate::messages::primitive_workflow_argument::Kind::BoolValue(*b)),
        })),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Kind::Primitive(PrimitiveWorkflowArgument {
                    kind: Some(crate::messages::primitive_workflow_argument::Kind::IntValue(i)),
                }))
            } else if let Some(f) = n.as_f64() {
                Some(Kind::Primitive(PrimitiveWorkflowArgument {
                    kind: Some(crate::messages::primitive_workflow_argument::Kind::DoubleValue(f)),
                }))
            } else {
                None
            }
        }
        serde_json::Value::String(s) => Some(Kind::Primitive(PrimitiveWorkflowArgument {
            kind: Some(crate::messages::primitive_workflow_argument::Kind::StringValue(s.clone())),
        })),
        serde_json::Value::Array(arr) => {
            let items: Vec<WorkflowArgumentValue> = arr.iter().map(json_to_proto_value).collect();
            Some(Kind::ListValue(crate::messages::WorkflowListArgument { items }))
        }
        serde_json::Value::Object(obj) => {
            let entries: Vec<WorkflowArgument> = obj
                .iter()
                .map(|(k, v)| WorkflowArgument {
                    key: k.clone(),
                    value: Some(json_to_proto_value(v)),
                })
                .collect();
            Some(Kind::DictValue(crate::messages::WorkflowDictArgument { entries }))
        }
    };

    WorkflowArgumentValue { kind }
}

/// Convert a JSON object to proto WorkflowArguments.
fn json_to_proto_args(value: &serde_json::Value) -> Option<WorkflowArguments> {
    if let serde_json::Value::Object(obj) = value {
        let arguments: Vec<WorkflowArgument> = obj
            .iter()
            .map(|(k, v)| WorkflowArgument {
                key: k.clone(),
                value: Some(json_to_proto_value(v)),
            })
            .collect();
        Some(WorkflowArguments { arguments })
    } else {
        None
    }
}

/// Configuration for a single host coordinator.
#[derive(Debug, Clone)]
pub struct HostConfig {
    /// HTTP server bind address (for workflow submission)
    pub http_addr: SocketAddr,

    /// gRPC server bind address for action workers
    pub action_grpc_addr: SocketAddr,

    /// gRPC server bind address for instance workers
    pub instance_grpc_addr: SocketAddr,

    /// Number of action worker processes to spawn
    pub action_worker_count: usize,

    /// Number of instance worker processes to spawn
    pub instance_worker_count: usize,

    /// Max concurrent actions per action worker
    pub max_concurrent_per_action_worker: usize,

    /// Max concurrent instances per instance worker
    pub max_concurrent_per_instance_worker: usize,

    /// Python modules to load (contain workflows and actions)
    pub user_modules: Vec<String>,

    /// Dispatcher poll interval in milliseconds
    pub poll_interval_ms: u64,

    /// Working directory for Python workers (must contain pyproject.toml)
    pub python_dir: Option<String>,
}

impl Default for HostConfig {
    fn default() -> Self {
        HostConfig {
            http_addr: "127.0.0.1:24117".parse().unwrap(),
            action_grpc_addr: "127.0.0.1:24118".parse().unwrap(),
            instance_grpc_addr: "127.0.0.1:24119".parse().unwrap(),
            action_worker_count: num_cpus::get(),
            instance_worker_count: 4,
            max_concurrent_per_action_worker: 10,
            max_concurrent_per_instance_worker: 5,
            user_modules: vec![],
            poll_interval_ms: 50,
            python_dir: None,
        }
    }
}

/// A host coordinator that manages workers and dispatches work.
pub struct HostCoordinator {
    config: HostConfig,
    db: Arc<Database>,

    // Bridge states
    action_bridge_state: Arc<ActionWorkerBridgeState>,
    instance_bridge_state: Arc<InstanceWorkerBridgeState>,

    // Worker pools
    action_worker_pool: Arc<WorkerPool>,
    instance_worker_pool: Arc<WorkerPool>,

    // Running task handles
    handles: Vec<JoinHandle<()>>,
}

impl HostCoordinator {
    /// Create a new host coordinator.
    pub fn new(config: HostConfig, db: Arc<Database>) -> Self {
        let action_bridge_state =
            Arc::new(ActionWorkerBridgeState::new(config.max_concurrent_per_action_worker));
        let instance_bridge_state = Arc::new(InstanceWorkerBridgeState::new(
            config.max_concurrent_per_instance_worker,
        ));

        let action_worker_pool = Arc::new(WorkerPool::new(
            WorkerType::Action,
            config.action_grpc_addr.to_string(),
            config.user_modules.clone(),
            config.python_dir.clone(),
            config.action_worker_count,
        ));

        let instance_worker_pool = Arc::new(WorkerPool::new(
            WorkerType::Instance,
            config.instance_grpc_addr.to_string(),
            config.user_modules.clone(),
            config.python_dir.clone(),
            config.instance_worker_count,
        ));

        HostCoordinator {
            config,
            db,
            action_bridge_state,
            instance_bridge_state,
            action_worker_pool,
            instance_worker_pool,
            handles: Vec::new(),
        }
    }

    /// Start all services and background tasks.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        // Start gRPC servers
        self.start_grpc_servers().await?;

        // Start worker pools
        self.action_worker_pool.start().await?;
        self.instance_worker_pool.start().await?;

        // Start background tasks
        self.start_dispatcher_tasks();
        self.start_maintenance_tasks();
        self.start_recovery_tasks();

        info!(
            http_addr = %self.config.http_addr,
            action_grpc_addr = %self.config.action_grpc_addr,
            instance_grpc_addr = %self.config.instance_grpc_addr,
            action_workers = self.config.action_worker_count,
            instance_workers = self.config.instance_worker_count,
            "Host coordinator started"
        );

        Ok(())
    }

    /// Start HTTP and gRPC servers.
    async fn start_grpc_servers(&mut self) -> anyhow::Result<()> {
        // HTTP server
        let http_addr = self.config.http_addr;
        let http_state = HttpState {
            db: self.db.clone(),
        };
        let http_router = create_router(http_state);

        let http_handle = tokio::spawn(async move {
            let listener = TcpListener::bind(http_addr).await.unwrap();
            if let Err(e) = axum::serve(listener, http_router).await {
                error!(error = %e, "HTTP server error");
            }
        });
        self.handles.push(http_handle);

        // Action gRPC server
        let action_grpc_addr = self.config.action_grpc_addr;
        let action_bridge_service =
            ActionWorkerBridgeService::new(self.action_bridge_state.clone(), self.db.clone());

        let action_grpc_handle = tokio::spawn(async move {
            if let Err(e) = TonicServer::builder()
                .add_service(ActionWorkerBridgeServer::new(action_bridge_service))
                .serve(action_grpc_addr)
                .await
            {
                error!(error = %e, "Action gRPC server error");
            }
        });
        self.handles.push(action_grpc_handle);

        // Instance gRPC server
        let instance_grpc_addr = self.config.instance_grpc_addr;
        let instance_bridge_service =
            InstanceWorkerBridgeService::new(self.instance_bridge_state.clone(), self.db.clone());

        let instance_grpc_handle = tokio::spawn(async move {
            if let Err(e) = TonicServer::builder()
                .add_service(InstanceWorkerBridgeServer::new(instance_bridge_service))
                .serve(instance_grpc_addr)
                .await
            {
                error!(error = %e, "Instance gRPC server error");
            }
        });
        self.handles.push(instance_grpc_handle);

        Ok(())
    }

    /// Start dispatcher tasks that poll DB and dispatch to workers.
    fn start_dispatcher_tasks(&mut self) {
        let poll_interval = std::time::Duration::from_millis(self.config.poll_interval_ms);

        // Action dispatcher
        let action_db = self.db.clone();
        let action_state = self.action_bridge_state.clone();
        let action_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(poll_interval).await;

                let available = action_state.available_slots().await;
                if available == 0 {
                    continue;
                }

                match action_db.claim_queued_actions(available as i32).await {
                    Ok(actions) => {
                        if !actions.is_empty() {
                            info!(count = actions.len(), "Claimed queued actions for dispatch");
                        }
                        for action in actions {
                            let dispatch = ActionDispatch {
                                action_id: action.id.to_string(),
                                instance_id: action.instance_id.to_string(),
                                sequence: action.sequence as u32,
                                action_name: action.action_name,
                                module_name: action.module_name,
                                kwargs: json_to_proto_args(&action.kwargs),
                                timeout_seconds: Some(action.timeout_seconds as u32),
                                max_retries: Some(action.max_attempts as u32),
                                attempt_number: Some(action.attempt_count as u32),
                                dispatch_token: action.dispatch_token.map(|t| t.to_string()),
                            };

                            if let Err(e) = action_state.dispatch_action(dispatch).await {
                                error!(error = %e, "Failed to dispatch action");
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to claim queued actions");
                    }
                }
            }
        });
        self.handles.push(action_handle);

        // Instance dispatcher
        let instance_db = self.db.clone();
        let instance_state = self.instance_bridge_state.clone();
        let instance_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(poll_interval).await;

                let available = instance_state.available_slots().await;
                if available == 0 {
                    continue;
                }

                match instance_db.claim_ready_instances(available as i32).await {
                    Ok(instances) => {
                        for instance in instances {
                            let completed_actions = match instance_db
                                .get_completed_actions(instance.id)
                                .await
                            {
                                Ok(actions) => actions,
                                Err(e) => {
                                    error!(error = %e, "Failed to get completed actions");
                                    continue;
                                }
                            };

                            let dispatch = InstanceDispatch {
                                instance_id: instance.id.to_string(),
                                workflow_name: instance.workflow_name.clone(),
                                module_name: instance.module_name.clone(),
                                actions_until_index: instance.actions_until_index as u32,
                                initial_args: json_to_proto_args(&instance.initial_args),
                                completed_actions: completed_actions
                                    .into_iter()
                                    .map(|a| {
                                        // Convert stored JSON result to proto WorkflowArguments
                                        let payload = a.result.as_ref().and_then(|r| json_to_proto_args(r));
                                        ActionResult {
                                            action_id: a.id.to_string(),
                                            success: a.status == ActionStatus::Completed,
                                            payload,
                                            worker_start_ns: 0,
                                            worker_end_ns: 0,
                                            dispatch_token: None,
                                            error_type: None,
                                            error_message: a.error_message,
                                        }
                                    })
                                    .collect(),
                                scheduled_at_ms: instance
                                    .scheduled_at
                                    .map(|t| t.timestamp_millis())
                                    .unwrap_or(0),
                                dispatch_token: instance.dispatch_token.map(|t| t.to_string()),
                            };

                            if let Err(e) = instance_state.dispatch_instance(dispatch).await {
                                error!(error = %e, "Failed to dispatch instance");
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to claim ready instances");
                    }
                }
            }
        });
        self.handles.push(instance_handle);
    }

    /// Start worker pool maintenance tasks.
    fn start_maintenance_tasks(&mut self) {
        let action_pool = self.action_worker_pool.clone();
        let instance_pool = self.instance_worker_pool.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                if let Err(e) = action_pool.maintain().await {
                    error!(error = %e, "Failed to maintain action worker pool");
                }
                if let Err(e) = instance_pool.maintain().await {
                    error!(error = %e, "Failed to maintain instance worker pool");
                }
            }
        });
        self.handles.push(handle);
    }

    /// Start timeout recovery tasks.
    fn start_recovery_tasks(&mut self) {
        let db = self.db.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;

                match db.recover_timed_out_actions().await {
                    Ok(count) if count > 0 => {
                        info!(count, "Recovered timed-out actions");
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to recover timed-out actions");
                    }
                    _ => {}
                }

                match db.fail_exhausted_actions().await {
                    Ok(count) if count > 0 => {
                        info!(count, "Failed exhausted actions");
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to mark exhausted actions");
                    }
                    _ => {}
                }

                match db.recover_timed_out_instances().await {
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
        self.handles.push(handle);
    }

    /// Stop all services and background tasks.
    pub async fn stop(&mut self) {
        info!("Stopping host coordinator");

        // Abort all background tasks
        for handle in self.handles.drain(..) {
            handle.abort();
        }

        // Stop worker pools
        self.action_worker_pool.stop().await;
        self.instance_worker_pool.stop().await;
    }

    /// Get current capacity stats.
    pub async fn stats(&self) -> HostStats {
        HostStats {
            action_workers_connected: self.action_bridge_state.worker_count().await,
            instance_workers_connected: self.instance_bridge_state.worker_count().await,
            action_slots_available: self.action_bridge_state.available_slots().await,
            instance_slots_available: self.instance_bridge_state.available_slots().await,
        }
    }
}

/// Statistics for a host coordinator.
#[derive(Debug, Clone)]
pub struct HostStats {
    pub action_workers_connected: usize,
    pub instance_workers_connected: usize,
    pub action_slots_available: usize,
    pub instance_slots_available: usize,
}
