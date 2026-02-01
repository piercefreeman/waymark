//! In-memory workflow runner used for ExecuteWorkflow streams.

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
    DAGConverter, DAGNode, ExpressionEvaluator, inline_executor,
    ir_validation::validate_program,
    messages::ast as ir_ast,
    messages::proto,
    messages::{decode_message, encode_message},
    value::WorkflowValue,
    workflow_ir::log_workflow_ir,
    workflow_state::{Completion, ExecutionState, SLEEP_WORKER_ID},
};

/// gRPC stream item type for ExecuteWorkflow.
pub type WorkflowStreamResult = Result<proto::WorkflowStreamResponse, tonic::Status>;
pub type WorkflowStream = Pin<Box<dyn Stream<Item = WorkflowStreamResult> + Send + 'static>>;

/// Run an in-memory gRPC server with no database backing.
pub async fn run_in_memory_server(grpc_addr: SocketAddr) -> Result<()> {
    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind grpc listener on {grpc_addr}"))?;

    info!(?grpc_addr, "rappel in-memory bridge server listening");

    run_in_memory_grpc_server(grpc_listener).await
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
struct InMemoryWorkflowGrpcService;

impl InMemoryWorkflowGrpcService {
    fn new() -> Self {
        Self
    }
}

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
        worker_duration_ms: Some(duration_ms),
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
                let _ = instance
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
            let result = inline_executor::execute_inline_node(
                &mut instance.state,
                &instance.dag,
                &node_id,
                &dag_node,
            );
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

pub async fn execute_workflow_stream(
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
