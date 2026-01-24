//! Server/Client infrastructure for the Rappel workflow engine.
//!
//! This module provides:
//! - gRPC server for workflow registration and status (WorkflowService)
//! - gRPC health check for singleton discovery
//! - Configuration and server lifecycle management

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    pin::Pin,
    time::Duration,
};

use anyhow::{Context, Result};
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    DAGConverter, DAGHelper, DAGNode, ExpressionEvaluator,
    db::{Database, ScheduleType},
    execution_graph::{Completion, ExecutionState, SLEEP_WORKER_ID},
    ir_printer,
    ir_validation::validate_program,
    messages::ast as ir_ast,
    messages::proto,
    messages::{decode_message, encode_message},
    schedule::{apply_jitter, next_cron_run, next_interval_run},
    value::{WorkflowValue, workflow_value_from_proto_bytes, workflow_value_to_proto_bytes},
};

/// Service name for health checks
pub const SERVICE_NAME: &str = "rappel";

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub grpc_addr: SocketAddr,
    pub database_url: String,
}

/// Run the gRPC server with health check and workflow services
pub async fn run_server(config: ServerConfig) -> Result<()> {
    let ServerConfig {
        grpc_addr,
        database_url,
    } = config;

    let database = Database::connect(&database_url)
        .await
        .with_context(|| format!("failed to connect to database at {}", database_url))?;

    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind grpc listener on {grpc_addr}"))?;

    info!(?grpc_addr, "rappel bridge server listening");

    run_grpc_server(grpc_listener, database).await
}

/// Run an in-memory gRPC server with no database backing.
pub async fn run_in_memory_server(grpc_addr: SocketAddr) -> Result<()> {
    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind grpc listener on {grpc_addr}"))?;

    info!(?grpc_addr, "rappel in-memory bridge server listening");

    run_in_memory_grpc_server(grpc_listener).await
}

// ============================================================================
// gRPC Server
// ============================================================================

async fn run_grpc_server(listener: TcpListener, database: Database) -> Result<()> {
    use proto::workflow_service_server::WorkflowServiceServer;

    // Set up health reporting
    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<WorkflowServiceServer<WorkflowGrpcService>>()
        .await;

    let incoming = TcpListenerStream::new(listener);
    let service = WorkflowGrpcService::new(database);

    Server::builder()
        .add_service(health_service)
        .add_service(WorkflowServiceServer::new(service))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}

async fn run_in_memory_grpc_server(listener: TcpListener) -> Result<()> {
    use proto::workflow_service_server::WorkflowServiceServer;

    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<WorkflowServiceServer<InMemoryWorkflowGrpcService>>()
        .await;

    let incoming = TcpListenerStream::new(listener);
    let service = InMemoryWorkflowGrpcService::new();

    Server::builder()
        .add_service(health_service)
        .add_service(WorkflowServiceServer::new(service))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}

#[derive(Clone)]
struct WorkflowGrpcService {
    database: Database,
}

impl WorkflowGrpcService {
    fn new(database: Database) -> Self {
        Self { database }
    }
}

#[derive(Clone)]
struct InMemoryWorkflowGrpcService;

impl InMemoryWorkflowGrpcService {
    fn new() -> Self {
        Self
    }
}

/// Log a registered workflow's IR in a pretty-printed format
fn log_workflow_ir(workflow_name: &str, ir_hash: &str, program: &ir_ast::Program) {
    let ir_str = ir_printer::print_program(program);
    info!(
        workflow_name = %workflow_name,
        ir_hash = %ir_hash,
        "Registered workflow IR:\n{}",
        ir_str
    );
}

pub(crate) fn sanitize_interval(value: Option<f64>) -> Duration {
    let raw = value.unwrap_or(1.0);
    let clamped = raw.clamp(0.1, 30.0);
    Duration::from_secs_f64(clamped)
}

type WorkflowStreamResult = Result<proto::WorkflowStreamResponse, tonic::Status>;
type WorkflowStream = Pin<Box<dyn Stream<Item = WorkflowStreamResult> + Send + 'static>>;

struct InMemoryInstance {
    instance_id: Uuid,
    dag: crate::DAG,
    state: ExecutionState,
    in_flight: HashSet<String>,
    pending_completions: Vec<Completion>,
    action_seq: u32,
}

async fn run_in_memory_workflow(
    registration: proto::WorkflowRegistration,
    skip_sleep: bool,
    mut action_rx: mpsc::Receiver<proto::ActionResult>,
    response_tx: mpsc::Sender<WorkflowStreamResult>,
) -> Result<(), tonic::Status> {
    let program = ir_ast::Program::decode(&registration.ir[..])
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid IR: {e}")))?;
    if let Err(err) = validate_program(&program) {
        return Err(tonic::Status::invalid_argument(format!(
            "invalid IR: {err}"
        )));
    }

    log_workflow_ir(&registration.workflow_name, &registration.ir_hash, &program);

    let ast_program = decode_message::<crate::parser::ast::Program>(&registration.ir)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid IR: {e}")))?;
    let dag = DAGConverter::new()
        .convert(&ast_program)
        .map_err(|e| tonic::Status::invalid_argument(format!("dag conversion failed: {e}")))?;

    let inputs = registration.initial_context.unwrap_or_default();
    let mut state = ExecutionState::new();
    state.initialize_from_dag(&dag, &inputs);

    let mut instance = InMemoryInstance {
        instance_id: Uuid::new_v4(),
        dag,
        state,
        in_flight: HashSet::new(),
        pending_completions: Vec::new(),
        action_seq: 0,
    };

    let (sleep_tx, mut sleep_rx) = mpsc::channel(128);

    loop {
        drain_action_results(&mut instance, &mut action_rx);
        drain_sleep_completions(&mut instance, &mut sleep_rx);

        dispatch_ready_nodes(&mut instance, &response_tx, &sleep_tx, skip_sleep).await?;

        if !instance.pending_completions.is_empty() {
            let completions = std::mem::take(&mut instance.pending_completions);
            let result = instance
                .state
                .apply_completions_batch(completions, &instance.dag);

            if result.workflow_completed || result.workflow_failed {
                let payload = if let Some(payload) = result.result_payload {
                    payload
                } else if let Some(message) = result.error_message {
                    build_error_payload(&message)
                } else if result.workflow_failed {
                    build_error_payload("workflow failed")
                } else {
                    build_null_result_payload()
                };

                let response = proto::WorkflowStreamResponse {
                    kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                        proto::WorkflowExecutionResult { payload },
                    )),
                };
                let _ = response_tx.send(Ok(response)).await;
                break;
            }
        }

        if instance.state.graph.ready_queue.is_empty()
            && instance.pending_completions.is_empty()
            && instance.in_flight.is_empty()
        {
            let payload = build_error_payload("workflow stalled without pending work");
            let response = proto::WorkflowStreamResponse {
                kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                    proto::WorkflowExecutionResult { payload },
                )),
            };
            let _ = response_tx.send(Ok(response)).await;
            break;
        }

        if instance.state.graph.ready_queue.is_empty() && instance.pending_completions.is_empty() {
            tokio::select! {
                Some(action_result) = action_rx.recv() => {
                    handle_action_result(&mut instance, action_result);
                }
                Some(completion) = sleep_rx.recv() => {
                    handle_completion(&mut instance, completion);
                }
                else => {
                    let payload = build_error_payload("workflow stream closed unexpectedly");
                    let response = proto::WorkflowStreamResponse {
                        kind: Some(proto::workflow_stream_response::Kind::WorkflowResult(
                            proto::WorkflowExecutionResult { payload },
                        )),
                    };
                    let _ = response_tx.send(Ok(response)).await;
                    break;
                }
            }
        }
    }

    Ok(())
}

fn drain_action_results(
    instance: &mut InMemoryInstance,
    action_rx: &mut mpsc::Receiver<proto::ActionResult>,
) {
    while let Ok(result) = action_rx.try_recv() {
        handle_action_result(instance, result);
    }
}

fn drain_sleep_completions(
    instance: &mut InMemoryInstance,
    sleep_rx: &mut mpsc::Receiver<Completion>,
) {
    while let Ok(completion) = sleep_rx.try_recv() {
        handle_completion(instance, completion);
    }
}

fn handle_action_result(instance: &mut InMemoryInstance, result: proto::ActionResult) {
    let duration_ns = result.worker_end_ns.saturating_sub(result.worker_start_ns);
    let duration_ms = (duration_ns / 1_000_000) as i64;
    let payload_bytes = encode_message(
        result
            .payload
            .as_ref()
            .unwrap_or(&proto::WorkflowArguments::default()),
    );

    let completion = Completion {
        node_id: result.action_id,
        success: result.success,
        result: Some(payload_bytes),
        error: result.error_message.clone(),
        error_type: result.error_type.clone(),
        worker_id: "in_memory".to_string(),
        duration_ms,
        worker_duration_ms: Some(duration_ms), // In-memory execution time
    };

    handle_completion(instance, completion);
}

fn handle_completion(instance: &mut InMemoryInstance, completion: Completion) {
    instance.in_flight.remove(&completion.node_id);
    instance.pending_completions.push(completion);
}

async fn dispatch_ready_nodes(
    instance: &mut InMemoryInstance,
    response_tx: &mpsc::Sender<WorkflowStreamResult>,
    sleep_tx: &mpsc::Sender<Completion>,
    skip_sleep: bool,
) -> Result<(), tonic::Status> {
    let ready_nodes = instance.state.drain_ready_queue();

    for node_id in ready_nodes {
        if instance.in_flight.contains(&node_id) {
            continue;
        }

        let exec_node = match instance.state.graph.nodes.get(&node_id) {
            Some(node) => node.clone(),
            None => continue,
        };
        let template_id = exec_node.template_id.clone();
        let dag_node = match instance.dag.nodes.get(&template_id) {
            Some(node) => node.clone(),
            None => continue,
        };

        if dag_node.is_spread && exec_node.spread_index.is_none() {
            let mut completion_error = None;
            let items = match dag_node.spread_collection_expr.as_ref() {
                Some(expr) => match ExpressionEvaluator::evaluate(
                    expr,
                    &instance.state.build_scope_for_node(&node_id),
                ) {
                    Ok(WorkflowValue::List(items)) | Ok(WorkflowValue::Tuple(items)) => items,
                    Ok(WorkflowValue::Null) => Vec::new(),
                    Ok(other) => {
                        completion_error = Some(format!(
                            "Spread collection must be list or tuple, got {:?}",
                            other
                        ));
                        Vec::new()
                    }
                    Err(e) => {
                        completion_error =
                            Some(format!("Spread collection evaluation error: {}", e));
                        Vec::new()
                    }
                },
                None => {
                    completion_error =
                        Some("Spread node missing collection expression".to_string());
                    Vec::new()
                }
            };

            if completion_error.is_none() {
                instance
                    .state
                    .expand_spread(&template_id, items, &instance.dag);
            }

            let completion = Completion {
                node_id: node_id.clone(),
                success: completion_error.is_none(),
                result: None,
                error: completion_error.clone(),
                error_type: completion_error
                    .as_ref()
                    .map(|_| "SpreadEvaluationError".to_string()),
                worker_id: "inline".to_string(),
                duration_ms: 0,
                worker_duration_ms: Some(0),
            };
            instance.pending_completions.push(completion);
            continue;
        }

        if dag_node.action_name.as_deref() == Some("sleep") {
            schedule_sleep_action(instance, &node_id, &dag_node, skip_sleep, sleep_tx).await;
            continue;
        }

        if dag_node.module_name.is_some() && dag_node.action_name.is_some() {
            dispatch_action(instance, &node_id, &dag_node, response_tx).await?;
        } else {
            let result = execute_inline_node(instance, &node_id, &dag_node);
            let completion = Completion {
                node_id: node_id.clone(),
                success: result.is_ok(),
                result: result.ok(),
                error: None,
                error_type: None,
                worker_id: "inline".to_string(),
                duration_ms: 0,
                worker_duration_ms: Some(0),
            };
            instance.pending_completions.push(completion);
        }
    }

    Ok(())
}

async fn dispatch_action(
    instance: &mut InMemoryInstance,
    node_id: &str,
    dag_node: &DAGNode,
    response_tx: &mpsc::Sender<WorkflowStreamResult>,
) -> Result<(), tonic::Status> {
    let module_name = dag_node
        .module_name
        .clone()
        .ok_or_else(|| tonic::Status::invalid_argument("missing module name"))?;
    let action_name = dag_node
        .action_name
        .clone()
        .ok_or_else(|| tonic::Status::invalid_argument("missing action name"))?;

    let inputs_bytes = instance.state.get_inputs_for_node(node_id, &instance.dag);
    let inputs: proto::WorkflowArguments = inputs_bytes
        .as_ref()
        .and_then(|bytes| decode_message(bytes).ok())
        .unwrap_or_default();

    let timeout_seconds = instance.state.get_timeout_seconds(node_id);
    let max_retries = instance.state.get_max_retries(node_id);
    let attempt_number = instance.state.get_attempt_number(node_id);

    let dispatch = proto::ActionDispatch {
        action_id: node_id.to_string(),
        instance_id: instance.instance_id.to_string(),
        sequence: instance.action_seq,
        action_name,
        module_name,
        kwargs: Some(inputs),
        timeout_seconds: Some(timeout_seconds),
        max_retries: Some(max_retries),
        attempt_number: Some(attempt_number),
        dispatch_token: Some(Uuid::new_v4().to_string()),
    };
    instance.action_seq = instance.action_seq.wrapping_add(1);

    instance
        .state
        .mark_running(node_id, "in_memory", inputs_bytes);
    instance.in_flight.insert(node_id.to_string());

    let response = proto::WorkflowStreamResponse {
        kind: Some(proto::workflow_stream_response::Kind::ActionDispatch(
            dispatch,
        )),
    };

    response_tx
        .send(Ok(response))
        .await
        .map_err(|_| tonic::Status::cancelled("workflow stream closed"))?;

    Ok(())
}

async fn schedule_sleep_action(
    instance: &mut InMemoryInstance,
    node_id: &str,
    _dag_node: &DAGNode,
    skip_sleep: bool,
    sleep_tx: &mpsc::Sender<Completion>,
) {
    let inputs_bytes = instance.state.get_inputs_for_node(node_id, &instance.dag);
    let inputs = decode_workflow_arguments(inputs_bytes.as_deref());
    let duration_ms = if skip_sleep {
        0
    } else {
        sleep_duration_ms_from_args(&inputs)
    };

    if duration_ms <= 0 {
        let completion = Completion {
            node_id: node_id.to_string(),
            success: true,
            result: Some(build_null_result_payload()),
            error: None,
            error_type: None,
            worker_id: SLEEP_WORKER_ID.to_string(),
            duration_ms: 0,
            worker_duration_ms: Some(0),
        };
        instance.pending_completions.push(completion);
        return;
    }

    instance
        .state
        .mark_running(node_id, SLEEP_WORKER_ID, inputs_bytes);
    instance.in_flight.insert(node_id.to_string());

    let node_id = node_id.to_string();
    let sleep_tx = sleep_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(duration_ms as u64)).await;
        let completion = Completion {
            node_id,
            success: true,
            result: Some(build_null_result_payload()),
            error: None,
            error_type: None,
            worker_id: SLEEP_WORKER_ID.to_string(),
            duration_ms: duration_ms as i64,
            worker_duration_ms: Some(duration_ms as i64),
        };
        let _ = sleep_tx.send(completion).await;
    });
}

fn decode_workflow_arguments(bytes: Option<&[u8]>) -> proto::WorkflowArguments {
    bytes
        .and_then(|b| decode_message(b).ok())
        .unwrap_or_default()
}

fn sleep_duration_ms_from_args(inputs: &proto::WorkflowArguments) -> i64 {
    let mut duration_secs: Option<f64> = None;
    for arg in &inputs.arguments {
        if arg.key != "duration" && arg.key != "seconds" {
            continue;
        }
        let Some(value) = arg.value.as_ref() else {
            continue;
        };
        match WorkflowValue::from_proto(value) {
            WorkflowValue::Int(i) => {
                duration_secs = Some(i as f64);
            }
            WorkflowValue::Float(f) => {
                duration_secs = Some(f);
            }
            WorkflowValue::String(s) => {
                if let Ok(parsed) = s.parse::<f64>() {
                    duration_secs = Some(parsed);
                }
            }
            _ => {}
        }
        break;
    }

    let secs = duration_secs.unwrap_or(0.0);
    if secs <= 0.0 {
        0
    } else {
        (secs * 1000.0).ceil() as i64
    }
}

fn build_null_result_payload() -> Vec<u8> {
    let args = proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "result".to_string(),
            value: Some(WorkflowValue::Null.to_proto()),
        }],
    };
    encode_message(&args)
}

fn build_error_payload(message: &str) -> Vec<u8> {
    let mut values = HashMap::new();
    values.insert(
        "args".to_string(),
        WorkflowValue::Tuple(vec![WorkflowValue::String(message.to_string())]),
    );
    let error = WorkflowValue::Exception {
        exc_type: "RuntimeError".to_string(),
        module: "builtins".to_string(),
        message: message.to_string(),
        traceback: String::new(),
        values,
        type_hierarchy: vec![
            "RuntimeError".to_string(),
            "Exception".to_string(),
            "BaseException".to_string(),
        ],
    };
    let args = proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "error".to_string(),
            value: Some(error.to_proto()),
        }],
    };
    encode_message(&args)
}

fn execute_inline_node(
    instance: &mut InMemoryInstance,
    node_id: &str,
    dag_node: &DAGNode,
) -> Result<Vec<u8>, String> {
    let scope = instance.state.build_scope_for_node(node_id);

    match dag_node.node_type.as_str() {
        "assignment" | "fn_call" => {
            if let Some(assign_expr) = &dag_node.assign_expr {
                match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                    Ok(value) => {
                        let result_bytes = workflow_value_to_proto_bytes(&value);

                        if let Some(targets) = &dag_node.targets {
                            if targets.len() > 1 {
                                match &value {
                                    WorkflowValue::Tuple(items) | WorkflowValue::List(items) => {
                                        for (target, item) in targets.iter().zip(items.iter()) {
                                            instance.state.store_variable_for_node(
                                                node_id,
                                                &dag_node.node_type,
                                                target,
                                                item,
                                            );
                                        }
                                    }
                                    _ => {
                                        for target in targets {
                                            instance.state.store_variable_for_node(
                                                node_id,
                                                &dag_node.node_type,
                                                target,
                                                &value,
                                            );
                                        }
                                    }
                                }
                            } else {
                                for target in targets {
                                    instance.state.store_variable_for_node(
                                        node_id,
                                        &dag_node.node_type,
                                        target,
                                        &value,
                                    );
                                }
                            }
                        } else if let Some(target) = &dag_node.target {
                            instance.state.store_variable_for_node(
                                node_id,
                                &dag_node.node_type,
                                target,
                                &value,
                            );
                        }

                        Ok(result_bytes)
                    }
                    Err(e) => Err(format!("Assignment evaluation error: {}", e)),
                }
            } else {
                Ok(vec![])
            }
        }
        "branch" | "if" | "elif" => Ok(vec![]),
        "join" | "else" => Ok(vec![]),
        "aggregator" => {
            let helper = DAGHelper::new(&instance.dag);
            let exec_node = instance.state.graph.nodes.get(node_id);
            let source_is_spread = dag_node
                .aggregates_from
                .as_ref()
                .and_then(|id| instance.dag.nodes.get(id))
                .map(|n| n.is_spread)
                .unwrap_or(false);

            let source_ids: Vec<String> = if source_is_spread {
                exec_node.map(|n| n.waiting_for.clone()).unwrap_or_default()
            } else if let Some(exec_node) = exec_node
                && !exec_node.waiting_for.is_empty()
            {
                exec_node.waiting_for.clone()
            } else {
                helper
                    .get_incoming_edges(&dag_node.id)
                    .iter()
                    .filter(|edge| edge.edge_type == crate::dag::EdgeType::StateMachine)
                    .filter(|edge| edge.exception_types.is_none())
                    .map(|edge| edge.source.clone())
                    .collect()
            };

            let mut values = Vec::new();
            for source_id in source_ids {
                if let Some(source_node) = instance.state.graph.nodes.get(&source_id) {
                    if let Some(result_bytes) = &source_node.result {
                        let value =
                            extract_result_value(result_bytes).unwrap_or(WorkflowValue::Null);
                        values.push(value);
                    } else {
                        values.push(WorkflowValue::Null);
                    }
                }
            }

            let should_store_list = dag_node
                .targets
                .as_ref()
                .map(|targets| targets.len() == 1)
                .unwrap_or(false);

            if should_store_list {
                let list_value = WorkflowValue::List(values);
                let args = proto::WorkflowArguments {
                    arguments: vec![proto::WorkflowArgument {
                        key: "result".to_string(),
                        value: Some(list_value.to_proto()),
                    }],
                };
                Ok(encode_message(&args))
            } else {
                Ok(encode_message(&proto::WorkflowArguments {
                    arguments: vec![],
                }))
            }
        }
        "input" | "output" => Ok(vec![]),
        "return" => {
            if let Some(assign_expr) = &dag_node.assign_expr {
                match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                    Ok(value) => {
                        let result_bytes = workflow_value_to_proto_bytes(&value);
                        if let Some(target) = &dag_node.target {
                            instance.state.store_variable_for_node(
                                node_id,
                                &dag_node.node_type,
                                target,
                                &value,
                            );
                        }
                        Ok(result_bytes)
                    }
                    Err(e) => Err(format!("Return evaluation error: {}", e)),
                }
            } else {
                Ok(vec![])
            }
        }
        _ => Ok(vec![]),
    }
}

fn extract_result_value(result_bytes: &[u8]) -> Option<WorkflowValue> {
    if let Ok(args) = decode_message::<proto::WorkflowArguments>(result_bytes) {
        for arg in args.arguments {
            if arg.key == "result" {
                if let Some(value) = arg.value {
                    return Some(WorkflowValue::from_proto(&value));
                }
                return None;
            }
        }
    }

    workflow_value_from_proto_bytes(result_bytes)
}

async fn execute_workflow_stream(
    request: tonic::Request<tonic::Streaming<proto::WorkflowStreamRequest>>,
) -> Result<tonic::Response<WorkflowStream>, tonic::Status> {
    let mut inbound = request.into_inner();
    let first = inbound
        .message()
        .await
        .map_err(|e| tonic::Status::internal(format!("stream error: {e}")))?;

    let Some(first) = first else {
        return Err(tonic::Status::invalid_argument(
            "missing initial workflow registration",
        ));
    };

    let skip_sleep = first.skip_sleep;
    let registration = match first.kind {
        Some(proto::workflow_stream_request::Kind::Registration(registration)) => registration,
        Some(proto::workflow_stream_request::Kind::ActionResult(_)) => {
            return Err(tonic::Status::invalid_argument(
                "first stream message must be registration",
            ));
        }
        None => {
            return Err(tonic::Status::invalid_argument(
                "first stream message missing registration",
            ));
        }
    };

    let (action_tx, action_rx) = mpsc::channel(128);
    let (response_tx, response_rx) = mpsc::channel::<WorkflowStreamResult>(128);

    tokio::spawn(async move {
        loop {
            match inbound.message().await {
                Ok(Some(message)) => match message.kind {
                    Some(proto::workflow_stream_request::Kind::ActionResult(result)) => {
                        if action_tx.send(result).await.is_err() {
                            break;
                        }
                    }
                    Some(proto::workflow_stream_request::Kind::Registration(_)) | None => {}
                },
                Ok(None) => break,
                Err(err) => {
                    warn!(error = %err, "workflow stream inbound error");
                    break;
                }
            }
        }
    });

    tokio::spawn(async move {
        if let Err(status) =
            run_in_memory_workflow(registration, skip_sleep, action_rx, response_tx.clone()).await
        {
            let _ = response_tx.send(Err(status)).await;
        }
    });

    Ok(tonic::Response::new(
        Box::pin(ReceiverStream::new(response_rx)) as WorkflowStream,
    ))
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for WorkflowGrpcService {
    type ExecuteWorkflowStream = WorkflowStream;

    async fn register_workflow(
        &self,
        request: tonic::Request<proto::RegisterWorkflowRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowResponse>, tonic::Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| tonic::Status::invalid_argument("registration missing"))?;

        let program = ir_ast::Program::decode(&registration.ir[..])
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid IR: {e}")))?;
        if let Err(err) = validate_program(&program) {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid IR: {err}"
            )));
        }

        // Log the registered workflow IR
        log_workflow_ir(&registration.workflow_name, &registration.ir_hash, &program);

        // Register the workflow version
        let version_id = self
            .database
            .upsert_workflow_version(
                &registration.workflow_name,
                &registration.ir_hash,
                &registration.ir,
                registration.concurrent,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        // Create an instance with initial context if provided
        let initial_input = registration.initial_context.map(|ctx| ctx.encode_to_vec());
        let priority = registration.priority.unwrap_or(0);
        let instance_id = self
            .database
            .create_instance_with_priority(
                &registration.workflow_name,
                version_id,
                initial_input.as_deref(),
                None,
                priority,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        Ok(tonic::Response::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn register_workflow_batch(
        &self,
        request: tonic::Request<proto::RegisterWorkflowBatchRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowBatchResponse>, tonic::Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| tonic::Status::invalid_argument("registration missing"))?;

        let proto::WorkflowRegistration {
            workflow_name,
            ir,
            ir_hash,
            concurrent,
            initial_context,
            priority,
        } = registration;

        let program = ir_ast::Program::decode(&ir[..])
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid IR: {e}")))?;
        if let Err(err) = validate_program(&program) {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid IR: {err}"
            )));
        }

        log_workflow_ir(&workflow_name, &ir_hash, &program);

        let version_id = self
            .database
            .upsert_workflow_version(&workflow_name, &ir_hash, &ir, concurrent)
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        let priority = priority.unwrap_or(0);
        let batch_size = match inner.batch_size {
            0 => 500usize,
            size => size as usize,
        };

        let mut queued: u64 = 0;
        let mut instance_ids: Vec<String> = Vec::new();

        if !inner.inputs_list.is_empty() {
            for chunk in inner.inputs_list.chunks(batch_size) {
                let input_payloads: Vec<Option<Vec<u8>>> = chunk
                    .iter()
                    .map(|inputs| Some(inputs.encode_to_vec()))
                    .collect();
                if inner.include_instance_ids {
                    let ids = self
                        .database
                        .create_instances_batch_with_priority(
                            &workflow_name,
                            version_id,
                            &input_payloads,
                            None,
                            priority,
                        )
                        .await
                        .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;
                    queued += ids.len() as u64;
                    instance_ids.extend(ids.into_iter().map(|id| id.to_string()));
                } else {
                    let count = self
                        .database
                        .create_instances_batch_count_with_priority(
                            &workflow_name,
                            version_id,
                            &input_payloads,
                            None,
                            priority,
                        )
                        .await
                        .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;
                    queued += count as u64;
                }
            }
        } else {
            if inner.count == 0 {
                return Err(tonic::Status::invalid_argument(
                    "count must be >= 1 when inputs_list is empty",
                ));
            }
            let base_inputs = inner.inputs.or(initial_context);
            let base_payload = base_inputs.map(|inputs| inputs.encode_to_vec());
            let mut remaining = inner.count as usize;
            while remaining > 0 {
                let size = batch_size.min(remaining);
                let input_payloads = vec![base_payload.clone(); size];
                if inner.include_instance_ids {
                    let ids = self
                        .database
                        .create_instances_batch_with_priority(
                            &workflow_name,
                            version_id,
                            &input_payloads,
                            None,
                            priority,
                        )
                        .await
                        .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;
                    queued += ids.len() as u64;
                    instance_ids.extend(ids.into_iter().map(|id| id.to_string()));
                } else {
                    let count = self
                        .database
                        .create_instances_batch_count_with_priority(
                            &workflow_name,
                            version_id,
                            &input_payloads,
                            None,
                            priority,
                        )
                        .await
                        .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;
                    queued += count as u64;
                }
                remaining -= size;
            }
        }

        let queued_u32 = u32::try_from(queued)
            .map_err(|_| tonic::Status::invalid_argument("queued instance count exceeds u32"))?;

        Ok(tonic::Response::new(proto::RegisterWorkflowBatchResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_ids: instance_ids,
            queued: queued_u32,
        }))
    }

    async fn wait_for_instance(
        &self,
        request: tonic::Request<proto::WaitForInstanceRequest>,
    ) -> Result<tonic::Response<proto::WaitForInstanceResponse>, tonic::Status> {
        use crate::db::{DbError, WorkflowInstanceId};

        let inner = request.into_inner();
        let instance_id: uuid::Uuid = inner.instance_id.parse().map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid instance_id: {err}"))
        })?;

        let interval = sanitize_interval(Some(inner.poll_interval_secs));

        // Poll for instance completion
        loop {
            let result = self
                .database
                .get_instance(WorkflowInstanceId(instance_id))
                .await;

            match result {
                Ok(inst) if inst.status == "completed" => {
                    let payload = inst.result_payload.unwrap_or_default();
                    return Ok(tonic::Response::new(proto::WaitForInstanceResponse {
                        payload,
                    }));
                }
                Ok(inst) if inst.status == "failed" => {
                    return Err(tonic::Status::internal("workflow instance failed"));
                }
                Ok(_) => {
                    // Still running, wait and poll again
                    tokio::time::sleep(interval).await;
                }
                Err(DbError::NotFound(_)) => {
                    return Err(tonic::Status::not_found("instance not found"));
                }
                Err(e) => {
                    return Err(tonic::Status::internal(format!("database error: {e}")));
                }
            }
        }
    }

    async fn register_schedule(
        &self,
        request: tonic::Request<proto::RegisterScheduleRequest>,
    ) -> Result<tonic::Response<proto::RegisterScheduleResponse>, tonic::Status> {
        let inner = request.into_inner();

        if inner.schedule_name.is_empty() {
            return Err(tonic::Status::invalid_argument("schedule_name is required"));
        }

        if self
            .database
            .get_schedule_by_name(&inner.workflow_name, &inner.schedule_name)
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?
            .is_some()
        {
            return Err(tonic::Status::already_exists("schedule already exists"));
        }

        // If a workflow registration is provided, register the workflow version first.
        // This ensures the workflow DAG exists when the schedule fires.
        if let Some(ref registration) = inner.registration {
            let program = ir_ast::Program::decode(&registration.ir[..])
                .map_err(|e| tonic::Status::invalid_argument(format!("invalid IR: {e}")))?;
            if let Err(err) = validate_program(&program) {
                return Err(tonic::Status::invalid_argument(format!(
                    "invalid IR: {err}"
                )));
            }
            log_workflow_ir(&registration.workflow_name, &registration.ir_hash, &program);
            self.database
                .upsert_workflow_version(
                    &registration.workflow_name,
                    &registration.ir_hash,
                    &registration.ir,
                    registration.concurrent,
                )
                .await
                .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

            info!(
                workflow_name = %registration.workflow_name,
                ir_hash = %registration.ir_hash,
                "registered workflow version for scheduled execution"
            );
        }

        let schedule = inner
            .schedule
            .ok_or_else(|| tonic::Status::invalid_argument("schedule required"))?;

        let jitter_seconds = schedule.jitter_seconds;
        if jitter_seconds < 0 {
            return Err(tonic::Status::invalid_argument(
                "jitter_seconds must be non-negative",
            ));
        }

        // Validate and compute next_run_at
        let (schedule_type, cron_expr, interval_secs, next_run) = match schedule.r#type() {
            proto::ScheduleType::Cron => {
                let expr = &schedule.cron_expression;
                if expr.is_empty() {
                    return Err(tonic::Status::invalid_argument(
                        "cron_expression required for cron schedule",
                    ));
                }
                let next = next_cron_run(expr)
                    .and_then(|base| apply_jitter(base, jitter_seconds))
                    .map_err(tonic::Status::invalid_argument)?;
                (ScheduleType::Cron, Some(expr.as_str()), None, next)
            }
            proto::ScheduleType::Interval => {
                let secs = schedule.interval_seconds;
                if secs <= 0 {
                    return Err(tonic::Status::invalid_argument(
                        "interval_seconds must be positive",
                    ));
                }
                let next = apply_jitter(next_interval_run(secs, None), jitter_seconds)
                    .map_err(tonic::Status::invalid_argument)?;
                (ScheduleType::Interval, None, Some(secs), next)
            }
            proto::ScheduleType::Unspecified => {
                return Err(tonic::Status::invalid_argument("schedule type required"));
            }
        };

        let input_payload = inner.inputs.map(|i| i.encode_to_vec());
        let priority = inner.priority.unwrap_or(0);

        let schedule_id = self
            .database
            .upsert_schedule(
                &inner.workflow_name,
                &inner.schedule_name,
                schedule_type,
                cron_expr,
                interval_secs,
                jitter_seconds,
                input_payload.as_deref(),
                next_run,
                priority,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        info!(
            workflow_name = %inner.workflow_name,
            schedule_name = %inner.schedule_name,
            schedule_type = %schedule_type.as_str(),
            next_run_at = %next_run.to_rfc3339(),
            "registered workflow schedule"
        );

        Ok(tonic::Response::new(proto::RegisterScheduleResponse {
            schedule_id: schedule_id.to_string(),
            next_run_at: next_run.to_rfc3339(),
        }))
    }

    async fn update_schedule_status(
        &self,
        request: tonic::Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<tonic::Response<proto::UpdateScheduleStatusResponse>, tonic::Status> {
        let inner = request.into_inner();

        let status_str = match inner.status() {
            proto::ScheduleStatus::Active => "active",
            proto::ScheduleStatus::Paused => "paused",
            proto::ScheduleStatus::Unspecified => {
                return Err(tonic::Status::invalid_argument("status required"));
            }
        };

        // Validate schedule_name is provided
        if inner.schedule_name.is_empty() {
            return Err(tonic::Status::invalid_argument("schedule_name is required"));
        }

        let success = self
            .database
            .update_schedule_status(&inner.workflow_name, &inner.schedule_name, status_str)
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        info!(
            workflow_name = %inner.workflow_name,
            schedule_name = %inner.schedule_name,
            status = %status_str,
            success = success,
            "updated schedule status"
        );

        Ok(tonic::Response::new(proto::UpdateScheduleStatusResponse {
            success,
        }))
    }

    async fn delete_schedule(
        &self,
        request: tonic::Request<proto::DeleteScheduleRequest>,
    ) -> Result<tonic::Response<proto::DeleteScheduleResponse>, tonic::Status> {
        let inner = request.into_inner();

        // Validate schedule_name is provided
        if inner.schedule_name.is_empty() {
            return Err(tonic::Status::invalid_argument("schedule_name is required"));
        }

        let success = self
            .database
            .delete_schedule(&inner.workflow_name, &inner.schedule_name)
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        info!(
            workflow_name = %inner.workflow_name,
            schedule_name = %inner.schedule_name,
            success = success,
            "deleted schedule"
        );

        Ok(tonic::Response::new(proto::DeleteScheduleResponse {
            success,
        }))
    }

    async fn list_schedules(
        &self,
        request: tonic::Request<proto::ListSchedulesRequest>,
    ) -> Result<tonic::Response<proto::ListSchedulesResponse>, tonic::Status> {
        let inner = request.into_inner();

        let schedules = self
            .database
            .list_schedules(inner.status_filter.as_deref())
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        let schedule_infos: Vec<proto::ScheduleInfo> = schedules
            .into_iter()
            .map(|s| {
                let schedule_type = match s.schedule_type.as_str() {
                    "cron" => proto::ScheduleType::Cron,
                    "interval" => proto::ScheduleType::Interval,
                    _ => proto::ScheduleType::Unspecified,
                };
                let status = match s.status.as_str() {
                    "active" => proto::ScheduleStatus::Active,
                    "paused" => proto::ScheduleStatus::Paused,
                    _ => proto::ScheduleStatus::Unspecified,
                };
                proto::ScheduleInfo {
                    id: s.id.to_string(),
                    workflow_name: s.workflow_name,
                    schedule_name: s.schedule_name,
                    schedule_type: schedule_type.into(),
                    cron_expression: s.cron_expression.unwrap_or_default(),
                    interval_seconds: s.interval_seconds.unwrap_or(0),
                    status: status.into(),
                    next_run_at: s.next_run_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
                    last_run_at: s.last_run_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
                    last_instance_id: s
                        .last_instance_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    created_at: s.created_at.to_rfc3339(),
                    updated_at: s.updated_at.to_rfc3339(),
                    jitter_seconds: s.jitter_seconds,
                }
            })
            .collect();

        info!(count = schedule_infos.len(), "listed schedules");

        Ok(tonic::Response::new(proto::ListSchedulesResponse {
            schedules: schedule_infos,
        }))
    }

    async fn execute_workflow(
        &self,
        request: tonic::Request<tonic::Streaming<proto::WorkflowStreamRequest>>,
    ) -> Result<tonic::Response<Self::ExecuteWorkflowStream>, tonic::Status> {
        execute_workflow_stream(request).await
    }
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for InMemoryWorkflowGrpcService {
    type ExecuteWorkflowStream = WorkflowStream;

    async fn register_workflow(
        &self,
        _request: tonic::Request<proto::RegisterWorkflowRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "register_workflow is unavailable in in-memory mode",
        ))
    }

    async fn register_workflow_batch(
        &self,
        _request: tonic::Request<proto::RegisterWorkflowBatchRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowBatchResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "register_workflow_batch is unavailable in in-memory mode",
        ))
    }

    async fn wait_for_instance(
        &self,
        _request: tonic::Request<proto::WaitForInstanceRequest>,
    ) -> Result<tonic::Response<proto::WaitForInstanceResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "wait_for_instance is unavailable in in-memory mode",
        ))
    }

    async fn register_schedule(
        &self,
        _request: tonic::Request<proto::RegisterScheduleRequest>,
    ) -> Result<tonic::Response<proto::RegisterScheduleResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "register_schedule is unavailable in in-memory mode",
        ))
    }

    async fn update_schedule_status(
        &self,
        _request: tonic::Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<tonic::Response<proto::UpdateScheduleStatusResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "update_schedule_status is unavailable in in-memory mode",
        ))
    }

    async fn delete_schedule(
        &self,
        _request: tonic::Request<proto::DeleteScheduleRequest>,
    ) -> Result<tonic::Response<proto::DeleteScheduleResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "delete_schedule is unavailable in in-memory mode",
        ))
    }

    async fn list_schedules(
        &self,
        _request: tonic::Request<proto::ListSchedulesRequest>,
    ) -> Result<tonic::Response<proto::ListSchedulesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "list_schedules is unavailable in in-memory mode",
        ))
    }

    async fn execute_workflow(
        &self,
        request: tonic::Request<tonic::Streaming<proto::WorkflowStreamRequest>>,
    ) -> Result<tonic::Response<Self::ExecuteWorkflowStream>, tonic::Status> {
        execute_workflow_stream(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_interval() {
        // Test default
        assert_eq!(sanitize_interval(None), Duration::from_secs_f64(1.0));

        // Test clamping
        assert_eq!(sanitize_interval(Some(0.01)), Duration::from_secs_f64(0.1));
        assert_eq!(
            sanitize_interval(Some(100.0)),
            Duration::from_secs_f64(30.0)
        );
        assert_eq!(sanitize_interval(Some(5.0)), Duration::from_secs_f64(5.0));
    }
}
