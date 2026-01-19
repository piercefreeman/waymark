//! In-memory workflow execution for local broker gRPC streaming.
//!
//! This module executes workflow IR in memory and produces ActionDispatch
//! messages plus a final workflow result payload, mirroring the production
//! readiness model without requiring a database.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use anyhow::{Context, Result};
use prost::Message;
use tracing::warn;
use uuid::Uuid;

use crate::completion::{CompletionPlan, ReadinessIncrement};
use crate::dag::{DAG, convert_to_dag};
use crate::db::{BackoffKind, WorkflowInstanceId};
use crate::execution::context::ExecutionContext;
use crate::execution::engine::CompletionEngine;
use crate::execution::model::ExceptionHandlingOutcome;
use crate::ir_validation::validate_program;
use crate::messages::{ast as ir_ast, proto};
use crate::parser::ast;
use crate::value::WorkflowValue;

#[derive(Debug)]
pub struct ExecutionStep {
    pub dispatches: Vec<proto::ActionDispatch>,
    pub completed_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct ActionState {
    node_id: String,
    module_name: String,
    action_name: String,
    dispatch_payload: Vec<u8>,
    timeout_seconds: u32,
    max_retries: u32,
    backoff_kind: BackoffKind,
    backoff_base_delay_ms: u32,
    attempt_number: u32,
}

#[derive(Debug)]
enum ReadyNode {
    Action(ReadinessIncrement),
    Barrier(String),
    Sleep(ReadinessIncrement),
}

pub struct InMemoryWorkflowExecutor {
    dag: DAG,
    instance_id: WorkflowInstanceId,
    initial_scope: HashMap<String, WorkflowValue>,
    inbox: HashMap<String, HashMap<String, WorkflowValue>>,
    spread_inbox: HashMap<String, BTreeMap<i32, WorkflowValue>>,
    readiness_counts: HashMap<String, i32>,
    readiness_required: HashMap<String, i32>,
    action_states: HashMap<String, ActionState>,
    next_sequence: u32,
    skip_sleep: bool,
}

#[tonic::async_trait]
impl ExecutionContext for InMemoryWorkflowExecutor {
    fn dag(&self) -> &DAG {
        &self.dag
    }

    fn instance_id(&self) -> WorkflowInstanceId {
        self.instance_id
    }

    async fn initial_scope(&self) -> Result<HashMap<String, WorkflowValue>> {
        Ok(self.initial_scope.clone())
    }

    async fn load_inbox(
        &self,
        node_ids: &HashSet<String>,
    ) -> Result<HashMap<String, HashMap<String, WorkflowValue>>> {
        Ok(node_ids
            .iter()
            .filter_map(|node_id| {
                self.inbox
                    .get(node_id)
                    .map(|vars| (node_id.clone(), vars.clone()))
            })
            .collect())
    }

    async fn read_inbox(&self, node_id: &str) -> Result<HashMap<String, WorkflowValue>> {
        Ok(self.inbox.get(node_id).cloned().unwrap_or_default())
    }

    async fn read_spread_inbox(&self, node_id: &str) -> Result<Vec<(i32, WorkflowValue)>> {
        Ok(self
            .spread_inbox
            .get(node_id)
            .map(|entries| {
                entries
                    .iter()
                    .map(|(idx, value)| (*idx, value.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl InMemoryWorkflowExecutor {
    pub fn from_registration(
        registration: proto::WorkflowRegistration,
        skip_sleep: bool,
    ) -> Result<Self> {
        let program = ir_ast::Program::decode(&registration.ir[..]).context("invalid IR")?;
        validate_program(&program)
            .map_err(|err| anyhow::anyhow!(err))
            .context("invalid IR")?;

        let dag_program =
            ast::Program::decode(&registration.ir[..]).context("invalid IR for DAG conversion")?;
        let dag = convert_to_dag(&dag_program)
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to convert IR to DAG")?;

        let initial_scope = registration
            .initial_context
            .as_ref()
            .map(workflow_arguments_to_scope)
            .unwrap_or_default();

        Ok(Self {
            dag,
            instance_id: WorkflowInstanceId(Uuid::new_v4()),
            initial_scope,
            inbox: HashMap::new(),
            spread_inbox: HashMap::new(),
            readiness_counts: HashMap::new(),
            readiness_required: HashMap::new(),
            action_states: HashMap::new(),
            next_sequence: 0,
            skip_sleep,
        })
    }

    pub async fn start(&mut self) -> Result<ExecutionStep> {
        let engine_plan = CompletionEngine::build_start_plan(self).await?;
        self.initial_scope = engine_plan.initial_scope;
        self.apply_inbox_writes(engine_plan.seed_inbox_writes);
        self.apply_plan(engine_plan.plan).await
    }

    pub async fn handle_action_result(
        &mut self,
        result: proto::ActionResult,
    ) -> Result<ExecutionStep> {
        let action_id = result.action_id.clone();
        let state = match self.action_states.remove(&action_id) {
            Some(state) => state,
            None => {
                warn!(action_id = %action_id, "received action result for unknown action");
                return Ok(ExecutionStep {
                    dispatches: Vec::new(),
                    completed_payload: None,
                });
            }
        };

        let payload_bytes = result
            .payload
            .as_ref()
            .map(|payload| payload.encode_to_vec())
            .unwrap_or_default();

        if result.success {
            let engine_plan =
                CompletionEngine::build_success_plan(self, &state.node_id, &payload_bytes).await?;
            self.apply_plan(engine_plan.plan).await
        } else {
            match CompletionEngine::handle_action_failure(
                self,
                &state.node_id,
                &payload_bytes,
                state.attempt_number as i32,
                state.max_retries as i32,
            )
            .await?
            {
                ExceptionHandlingOutcome::Retry => {
                    let mut next_state = state.clone();
                    next_state.attempt_number += 1;
                    self.maybe_backoff(&next_state).await;
                    let dispatch = self.dispatch_action(&action_id, &next_state)?;
                    self.action_states.insert(action_id, next_state);
                    Ok(ExecutionStep {
                        dispatches: vec![dispatch],
                        completed_payload: None,
                    })
                }
                ExceptionHandlingOutcome::Handled { plan, .. } => self.apply_plan(*plan).await,
                ExceptionHandlingOutcome::Unhandled => {
                    let fallback = proto::WorkflowArguments {
                        arguments: Vec::new(),
                    }
                    .encode_to_vec();
                    Ok(ExecutionStep {
                        dispatches: Vec::new(),
                        completed_payload: Some(if payload_bytes.is_empty() {
                            fallback
                        } else {
                            payload_bytes
                        }),
                    })
                }
            }
        }
    }

    async fn apply_plan(&mut self, plan: CompletionPlan) -> Result<ExecutionStep> {
        let mut dispatches: Vec<proto::ActionDispatch> = Vec::new();
        let mut plan_queue: VecDeque<CompletionPlan> = VecDeque::new();
        plan_queue.push_back(plan);

        while let Some(plan) = plan_queue.pop_front() {
            self.apply_inbox_writes(plan.inbox_writes);

            for node_id in plan.readiness_resets {
                self.readiness_counts.insert(node_id, 0);
            }
            for init in plan.readiness_inits {
                self.readiness_required
                    .insert(init.node_id.clone(), init.required_count);
                self.readiness_counts.entry(init.node_id).or_insert(0);
            }

            if let Some(completion) = plan.instance_completion {
                return Ok(ExecutionStep {
                    dispatches: Vec::new(),
                    completed_payload: Some(completion.result_payload),
                });
            }

            let mut ready_nodes: Vec<ReadyNode> = Vec::new();

            for increment in plan.readiness_increments {
                self.readiness_required
                    .insert(increment.node_id.clone(), increment.required_count);

                if increment.required_count == 1 && !increment.is_aggregator {
                    ready_nodes.push(Self::ready_from_increment(increment));
                    continue;
                }

                let counter = self
                    .readiness_counts
                    .entry(increment.node_id.clone())
                    .or_insert(0);
                *counter += 1;
                if *counter >= increment.required_count {
                    ready_nodes.push(Self::ready_from_increment(increment));
                }
            }

            for barrier_id in plan.barrier_enqueues {
                ready_nodes.push(ReadyNode::Barrier(barrier_id));
            }

            for node in ready_nodes {
                match node {
                    ReadyNode::Action(increment) => {
                        let action_id = Uuid::new_v4().to_string();
                        let state = ActionState {
                            node_id: increment.node_id.clone(),
                            module_name: increment.module_name.unwrap_or_default(),
                            action_name: increment.action_name.unwrap_or_default(),
                            dispatch_payload: increment.dispatch_payload.unwrap_or_default(),
                            timeout_seconds: increment.timeout_seconds as u32,
                            max_retries: increment.max_retries as u32,
                            backoff_kind: increment.backoff_kind,
                            backoff_base_delay_ms: increment.backoff_base_delay_ms as u32,
                            attempt_number: 0,
                        };
                        let dispatch = self.dispatch_action(&action_id, &state)?;
                        self.action_states.insert(action_id, state);
                        dispatches.push(dispatch);
                    }
                    ReadyNode::Barrier(node_id) => {
                        let next_plan =
                            CompletionEngine::build_barrier_plan(self, &node_id).await?;
                        plan_queue.push_back(next_plan);
                    }
                    ReadyNode::Sleep(increment) => {
                        self.sleep_from_increment(&increment).await;
                        let next_plan =
                            CompletionEngine::build_success_plan(self, &increment.node_id, &[])
                                .await?;
                        plan_queue.push_back(next_plan.plan);
                    }
                }
            }
        }

        Ok(ExecutionStep {
            dispatches,
            completed_payload: None,
        })
    }

    async fn sleep_from_increment(&self, increment: &ReadinessIncrement) {
        if self.skip_sleep {
            return;
        }
        let duration_secs = increment
            .dispatch_payload
            .as_ref()
            .and_then(|payload| serde_json::from_slice::<serde_json::Value>(payload).ok())
            .and_then(|value| value.get("duration").cloned())
            .and_then(|value| value.as_f64().or_else(|| value.as_i64().map(|v| v as f64)))
            .unwrap_or(0.0);

        if duration_secs > 0.0 {
            let duration_ms = (duration_secs * 1000.0) as u64;
            tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
        }
    }

    async fn maybe_backoff(&self, state: &ActionState) {
        if self.skip_sleep {
            return;
        }
        let delay_ms = match state.backoff_kind {
            BackoffKind::None => 0,
            BackoffKind::Linear => state.backoff_base_delay_ms * (state.attempt_number + 1),
            BackoffKind::Exponential => state
                .backoff_base_delay_ms
                .saturating_mul(2_u32.pow(state.attempt_number)),
        };

        if delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms as u64)).await;
        }
    }

    fn dispatch_action(
        &mut self,
        action_id: &str,
        state: &ActionState,
    ) -> Result<proto::ActionDispatch> {
        let kwargs = json_bytes_to_workflow_args(&state.dispatch_payload);
        let dispatch = proto::ActionDispatch {
            action_id: action_id.to_string(),
            instance_id: self.instance_id.0.to_string(),
            sequence: self.next_sequence,
            action_name: state.action_name.clone(),
            module_name: state.module_name.clone(),
            kwargs: Some(kwargs),
            timeout_seconds: Some(state.timeout_seconds),
            max_retries: Some(state.max_retries),
            attempt_number: Some(state.attempt_number),
            dispatch_token: None,
        };
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(dispatch)
    }

    fn apply_inbox_writes(&mut self, inbox_writes: Vec<crate::completion::InboxWrite>) {
        for write in inbox_writes {
            if let Some(spread_index) = write.spread_index {
                let entry = self
                    .spread_inbox
                    .entry(write.target_node_id.clone())
                    .or_default();
                entry.insert(spread_index, write.value.clone());
                continue;
            }

            let entry = self.inbox.entry(write.target_node_id.clone()).or_default();
            entry.insert(write.variable_name.clone(), write.value.clone());
        }
    }

    fn ready_from_increment(increment: ReadinessIncrement) -> ReadyNode {
        match increment.node_type {
            crate::completion::NodeType::Action => ReadyNode::Action(increment),
            crate::completion::NodeType::Barrier => ReadyNode::Barrier(increment.node_id),
            crate::completion::NodeType::Sleep => ReadyNode::Sleep(increment),
        }
    }
}

fn workflow_arguments_to_scope(args: &proto::WorkflowArguments) -> HashMap<String, WorkflowValue> {
    args.arguments
        .iter()
        .filter_map(|arg| {
            arg.value
                .as_ref()
                .map(|value| (arg.key.clone(), WorkflowValue::from_proto(value)))
        })
        .collect()
}

fn json_bytes_to_workflow_args(payload: &[u8]) -> proto::WorkflowArguments {
    if payload.is_empty() {
        return proto::WorkflowArguments { arguments: vec![] };
    }

    let json: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse dispatch payload as JSON: {}", e);
            return proto::WorkflowArguments { arguments: vec![] };
        }
    };

    match json {
        serde_json::Value::Object(obj) => {
            let arguments: Vec<proto::WorkflowArgument> = obj
                .iter()
                .map(|(k, v)| proto::WorkflowArgument {
                    key: k.clone(),
                    value: Some(WorkflowValue::from_json(v).to_proto()),
                })
                .collect();
            proto::WorkflowArguments { arguments }
        }
        _ => {
            warn!("dispatch_payload is not a JSON object, expected kwargs");
            proto::WorkflowArguments { arguments: vec![] }
        }
    }
}
