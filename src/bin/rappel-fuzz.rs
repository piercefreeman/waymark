use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use prost::Message;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::TempDir;
use uuid::Uuid;

use rappel::ir_ast;
use rappel::messages::{
    decode_message, encode_message, json_to_workflow_argument_value, proto,
    workflow_argument_value_to_json,
};
use rappel::value::workflow_value_from_proto_bytes;
use rappel::workflow_arguments_to_json;
use rappel::{
    Completion, DAGConverter, DAGHelper, DAGNode, EdgeType, ExecutionState, ExpressionEvaluator,
    WorkflowValue, validate_program,
};

#[derive(Debug, Parser)]
#[command(name = "rappel-fuzz")]
#[command(about = "Fuzz Python vs Rust in-memory execution parity.")]
struct Cli {
    #[arg(long, default_value_t = 25)]
    cases: usize,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, default_value_t = 8)]
    max_steps: usize,
    #[arg(long)]
    keep: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArgKind {
    Int,
    ListInt,
}

#[derive(Debug, Clone)]
struct ActionDef {
    name: &'static str,
    args: &'static [(&'static str, ArgKind)],
    output: ArgKind,
}

#[derive(Debug, Clone)]
struct CallSpec {
    action: &'static str,
    args: Vec<(String, String)>,
    output_var: String,
    trace_id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ActionTrace {
    trace_id: String,
    inputs: Value,
    output: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OracleTrace {
    actions: Vec<ActionTrace>,
    workflow_output: Value,
}

#[derive(Debug, Clone)]
struct FuzzError {
    exc_type: String,
    message: String,
    module: String,
    type_hierarchy: Vec<String>,
}

#[derive(Debug)]
enum ActionOutcome {
    Ok(Value),
    Err(FuzzError),
}

#[derive(Debug)]
struct ExecutionStep {
    dispatches: Vec<proto::ActionDispatch>,
    completed_payload: Option<Vec<u8>>,
}

struct InMemoryWorkflowExecutor {
    instance_id: Uuid,
    dag: rappel::DAG,
    state: ExecutionState,
    in_flight: HashSet<String>,
    pending_completions: Vec<Completion>,
    action_seq: u32,
    skip_sleep: bool,
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

        let ast_program =
            decode_message::<rappel::ast::Program>(&registration.ir).context("invalid IR")?;
        let dag = DAGConverter::new()
            .convert(&ast_program)
            .map_err(|err| anyhow::anyhow!(err))
            .context("failed to convert IR to DAG")?;

        let inputs = registration.initial_context.unwrap_or_default();
        let mut state = ExecutionState::new();
        state.initialize_from_dag(&dag, &inputs);

        Ok(Self {
            instance_id: Uuid::new_v4(),
            dag,
            state,
            in_flight: HashSet::new(),
            pending_completions: Vec::new(),
            action_seq: 0,
            skip_sleep,
        })
    }

    pub async fn start(&mut self) -> Result<ExecutionStep> {
        self.advance().await
    }

    pub async fn handle_action_result(
        &mut self,
        result: proto::ActionResult,
    ) -> Result<ExecutionStep> {
        let duration_ns = result.worker_end_ns.saturating_sub(result.worker_start_ns);
        let duration_ms = (duration_ns / 1_000_000) as i64;
        let payload_bytes = encode_message(
            result
                .payload
                .as_ref()
                .unwrap_or(&proto::WorkflowArguments::default()),
        );

        self.in_flight.remove(&result.action_id);
        self.pending_completions.push(Completion {
            node_id: result.action_id,
            success: result.success,
            result: Some(payload_bytes),
            error: result.error_message,
            error_type: result.error_type,
            worker_id: "in_memory".to_string(),
            duration_ms,
            worker_duration_ms: None,
        });

        self.advance().await
    }

    async fn advance(&mut self) -> Result<ExecutionStep> {
        loop {
            let mut dispatches: Vec<proto::ActionDispatch> = Vec::new();
            self.dispatch_ready_nodes(&mut dispatches).await?;

            if !self.pending_completions.is_empty() {
                let completions = std::mem::take(&mut self.pending_completions);
                let result = self.state.apply_completions_batch(completions, &self.dag);

                if result.workflow_completed || result.workflow_failed {
                    let payload = if let Some(payload) = result.result_payload {
                        payload
                    } else if let Some(message) = result.error_message {
                        build_workflow_error_payload(&message)
                    } else if result.workflow_failed {
                        build_workflow_error_payload("workflow failed")
                    } else {
                        build_null_result_payload()
                    };

                    return Ok(ExecutionStep {
                        dispatches: Vec::new(),
                        completed_payload: Some(payload),
                    });
                }
            }

            if !dispatches.is_empty() {
                return Ok(ExecutionStep {
                    dispatches,
                    completed_payload: None,
                });
            }

            if self.state.graph.ready_queue.is_empty()
                && self.pending_completions.is_empty()
                && self.in_flight.is_empty()
            {
                bail!("workflow stalled without pending work");
            }
        }
    }

    async fn dispatch_ready_nodes(
        &mut self,
        dispatches: &mut Vec<proto::ActionDispatch>,
    ) -> Result<()> {
        let ready_nodes = self.state.drain_ready_queue();

        for node_id in ready_nodes {
            if self.in_flight.contains(&node_id) {
                continue;
            }

            let exec_node = match self.state.graph.nodes.get(&node_id) {
                Some(node) => node.clone(),
                None => continue,
            };
            let template_id = exec_node.template_id.clone();
            let dag_node = match self.dag.nodes.get(&template_id) {
                Some(node) => node.clone(),
                None => continue,
            };

            if dag_node.is_spread && exec_node.spread_index.is_none() {
                let mut completion_error = None;
                let items = match dag_node.spread_collection_expr.as_ref() {
                    Some(expr) => match ExpressionEvaluator::evaluate(
                        expr,
                        &self.state.build_scope_for_node(&node_id),
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
                    self.state.expand_spread(&template_id, items, &self.dag);
                }

                self.pending_completions.push(Completion {
                    node_id: node_id.clone(),
                    success: completion_error.is_none(),
                    result: None,
                    error: completion_error.clone(),
                    error_type: completion_error
                        .as_ref()
                        .map(|_| "SpreadEvaluationError".to_string()),
                    worker_id: "inline".to_string(),
                    duration_ms: 0,
                    worker_duration_ms: None,
                });
                continue;
            }

            if dag_node.action_name.as_deref() == Some("sleep") {
                self.handle_sleep_node(&node_id).await?;
                continue;
            }

            if dag_node.module_name.is_some() && dag_node.action_name.is_some() {
                let dispatch = self.build_action_dispatch(&node_id, &dag_node)?;
                dispatches.push(dispatch);
            } else {
                let result = execute_inline_node(self, &node_id, &dag_node);
                self.pending_completions.push(Completion {
                    node_id: node_id.clone(),
                    success: result.is_ok(),
                    result: result.ok(),
                    error: None,
                    error_type: None,
                    worker_id: "inline".to_string(),
                    duration_ms: 0,
                    worker_duration_ms: None,
                });
            }
        }

        Ok(())
    }

    fn build_action_dispatch(
        &mut self,
        node_id: &str,
        dag_node: &DAGNode,
    ) -> Result<proto::ActionDispatch> {
        let module_name = dag_node
            .module_name
            .clone()
            .context("missing module name")?;
        let action_name = dag_node
            .action_name
            .clone()
            .context("missing action name")?;

        let inputs_bytes = self.state.get_inputs_for_node(node_id, &self.dag);
        let inputs: proto::WorkflowArguments = inputs_bytes
            .as_ref()
            .and_then(|bytes| decode_message(bytes).ok())
            .unwrap_or_default();

        let timeout_seconds = self.state.get_timeout_seconds(node_id);
        let max_retries = self.state.get_max_retries(node_id);
        let attempt_number = self.state.get_attempt_number(node_id);

        let dispatch = proto::ActionDispatch {
            action_id: node_id.to_string(),
            instance_id: self.instance_id.to_string(),
            sequence: self.action_seq,
            action_name,
            module_name,
            kwargs: Some(inputs),
            timeout_seconds: Some(timeout_seconds),
            max_retries: Some(max_retries),
            attempt_number: Some(attempt_number),
            dispatch_token: None,
        };
        self.action_seq = self.action_seq.wrapping_add(1);

        self.state.mark_running(node_id, "in_memory", inputs_bytes);
        self.in_flight.insert(node_id.to_string());

        Ok(dispatch)
    }

    async fn handle_sleep_node(&mut self, node_id: &str) -> Result<()> {
        let inputs_bytes = self.state.get_inputs_for_node(node_id, &self.dag);
        let inputs = decode_workflow_arguments(inputs_bytes.as_deref());
        let duration_ms = if self.skip_sleep {
            0
        } else {
            sleep_duration_ms_from_args(&inputs)
        };

        if duration_ms <= 0 {
            self.pending_completions.push(Completion {
                node_id: node_id.to_string(),
                success: true,
                result: Some(build_null_result_payload()),
                error: None,
                error_type: None,
                worker_id: "sleep".to_string(),
                duration_ms: 0,
                worker_duration_ms: None,
            });
            return Ok(());
        }

        self.state.mark_running(node_id, "sleep", inputs_bytes);
        self.in_flight.insert(node_id.to_string());
        tokio::time::sleep(Duration::from_millis(duration_ms as u64)).await;
        self.in_flight.remove(node_id);
        self.pending_completions.push(Completion {
            node_id: node_id.to_string(),
            success: true,
            result: Some(build_null_result_payload()),
            error: None,
            error_type: None,
            worker_id: "sleep".to_string(),
            duration_ms: duration_ms as i64,
            worker_duration_ms: None,
        });

        Ok(())
    }
}

#[derive(Debug)]
struct FuzzCase {
    python_source: String,
    inputs: Value,
}

const ACTIONS: &[ActionDef] = &[
    ActionDef {
        name: "add",
        args: &[("a", ArgKind::Int), ("b", ArgKind::Int)],
        output: ArgKind::Int,
    },
    ActionDef {
        name: "mul",
        args: &[("a", ArgKind::Int), ("b", ArgKind::Int)],
        output: ArgKind::Int,
    },
    ActionDef {
        name: "negate",
        args: &[("value", ArgKind::Int)],
        output: ArgKind::Int,
    },
    ActionDef {
        name: "sum_list",
        args: &[("items", ArgKind::ListInt)],
        output: ArgKind::Int,
    },
    ActionDef {
        name: "len_list",
        args: &[("items", ArgKind::ListInt)],
        output: ArgKind::Int,
    },
    ActionDef {
        name: "make_list",
        args: &[
            ("a", ArgKind::Int),
            ("b", ArgKind::Int),
            ("c", ArgKind::Int),
        ],
        output: ArgKind::ListInt,
    },
    ActionDef {
        name: "append_list",
        args: &[("items", ArgKind::ListInt), ("value", ArgKind::Int)],
        output: ArgKind::ListInt,
    },
    ActionDef {
        name: "scale_list",
        args: &[("items", ArgKind::ListInt), ("factor", ArgKind::Int)],
        output: ArgKind::ListInt,
    },
    ActionDef {
        name: "concat_lists",
        args: &[("left", ArgKind::ListInt), ("right", ArgKind::ListInt)],
        output: ArgKind::ListInt,
    },
    ActionDef {
        name: "take_slice",
        args: &[("items", ArgKind::ListInt), ("end", ArgKind::Int)],
        output: ArgKind::ListInt,
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let seed = cli.seed.unwrap_or_else(rand::random);
    let mut rng = StdRng::seed_from_u64(seed);
    println!("fuzz seed: {seed}");

    for case_idx in 0..cli.cases {
        let case = generate_case(&mut rng, cli.max_steps);
        if let Err(err) = run_case(case_idx, &case, cli.keep).await {
            println!("case {case_idx} failed with seed {seed}: {err:#}");
            return Err(err);
        }
    }

    Ok(())
}

fn generate_case(rng: &mut StdRng, max_steps: usize) -> FuzzCase {
    let inputs = generate_inputs(rng);
    let mut int_vars = vec!["seed".to_string(), "factor".to_string()];
    let mut list_vars = vec!["items".to_string()];
    let mut lines: Vec<String> = Vec::new();
    let mut trace_counter: i64 = 1000;
    let mut var_counter = 0usize;

    let steps = rng.gen_range(2..=max_steps.max(4));
    for _ in 0..steps {
        emit_random_action(
            rng,
            &mut lines,
            2,
            &mut int_vars,
            &mut list_vars,
            &mut trace_counter,
            &mut var_counter,
        );
    }

    emit_if_block(
        rng,
        &mut lines,
        &mut int_vars,
        &mut list_vars,
        &mut trace_counter,
        &mut var_counter,
    );

    emit_for_loop(
        rng,
        &mut lines,
        &mut list_vars,
        &mut trace_counter,
        &mut var_counter,
    );

    if rng.gen_bool(0.6) {
        emit_while_loop(
            rng,
            &mut lines,
            &mut int_vars,
            &mut list_vars,
            &mut trace_counter,
            &mut var_counter,
        );
    }

    emit_try_except(
        rng,
        &mut lines,
        &mut int_vars,
        &mut list_vars,
        &mut trace_counter,
        &mut var_counter,
    );

    let tail_steps = rng.gen_range(1..=3);
    for _ in 0..tail_steps {
        emit_random_action(
            rng,
            &mut lines,
            2,
            &mut int_vars,
            &mut list_vars,
            &mut trace_counter,
            &mut var_counter,
        );
    }

    let return_var = choose_return_var(rng, &int_vars, &list_vars);
    push_line(&mut lines, 2, format!("return {return_var}"));

    FuzzCase {
        python_source: render_python_module(&lines),
        inputs,
    }
}

fn generate_inputs(rng: &mut StdRng) -> Value {
    let seed = rng.gen_range(-50..=50);
    let factor = rng.gen_range(1..=6);
    let items_len = rng.gen_range(1..=6);
    let mut items: Vec<i64> = Vec::with_capacity(items_len);
    while items.len() < items_len {
        let candidate = rng.gen_range(-10..=10);
        if !items.contains(&candidate) {
            items.push(candidate);
        }
    }
    serde_json::json!({
        "seed": seed,
        "items": items,
        "factor": factor,
    })
}

fn choose_action(
    rng: &mut StdRng,
    int_vars: &[String],
    list_vars: &[String],
) -> &'static ActionDef {
    let mut eligible: Vec<&ActionDef> = Vec::new();
    for action in ACTIONS {
        if action.args.iter().all(|(_, kind)| match kind {
            ArgKind::Int => !int_vars.is_empty(),
            ArgKind::ListInt => !list_vars.is_empty(),
        }) {
            eligible.push(action);
        }
    }
    let idx = rng.gen_range(0..eligible.len());
    eligible[idx]
}

fn choose_action_with_output(
    rng: &mut StdRng,
    int_vars: &[String],
    list_vars: &[String],
    output: ArgKind,
) -> &'static ActionDef {
    let mut eligible: Vec<&ActionDef> = ACTIONS
        .iter()
        .filter(|action| action.output == output)
        .filter(|action| {
            action.args.iter().all(|(_, kind)| match kind {
                ArgKind::Int => !int_vars.is_empty(),
                ArgKind::ListInt => !list_vars.is_empty(),
            })
        })
        .collect();
    if eligible.is_empty() {
        eligible = ACTIONS
            .iter()
            .filter(|action| action.output == ArgKind::Int)
            .collect();
    }
    let idx = rng.gen_range(0..eligible.len());
    eligible[idx]
}

fn build_call(
    rng: &mut StdRng,
    action: &ActionDef,
    int_vars: &[String],
    list_vars: &[String],
    trace_counter: &mut i64,
    var_counter: &mut usize,
) -> (CallSpec, ArgKind) {
    let trace_id = next_trace_id(trace_counter);
    let mut args: Vec<(String, String)> = Vec::new();
    for (name, kind) in action.args {
        let expr = match kind {
            ArgKind::Int => choose_int_expr(rng, int_vars),
            ArgKind::ListInt => choose_list_var(rng, list_vars),
        };
        args.push((name.to_string(), expr));
    }
    let output_var = next_var(var_counter);
    (
        CallSpec {
            action: action.name,
            args,
            output_var,
            trace_id,
        },
        action.output,
    )
}

fn build_call_with_output(
    rng: &mut StdRng,
    action: &ActionDef,
    int_vars: &[String],
    list_vars: &[String],
    trace_counter: &mut i64,
    output_var: String,
) -> (CallSpec, ArgKind) {
    let trace_id = next_trace_id(trace_counter);
    let mut args: Vec<(String, String)> = Vec::new();
    for (name, kind) in action.args {
        let expr = match kind {
            ArgKind::Int => choose_int_expr(rng, int_vars),
            ArgKind::ListInt => choose_list_var(rng, list_vars),
        };
        args.push((name.to_string(), expr));
    }
    (
        CallSpec {
            action: action.name,
            args,
            output_var,
            trace_id,
        },
        action.output,
    )
}

fn choose_int_expr(rng: &mut StdRng, int_vars: &[String]) -> String {
    if !int_vars.is_empty() && rng.gen_bool(0.7) {
        let idx = rng.gen_range(0..int_vars.len());
        int_vars[idx].clone()
    } else {
        rng.gen_range(-9..=9).to_string()
    }
}

fn choose_list_var(rng: &mut StdRng, list_vars: &[String]) -> String {
    let idx = rng.gen_range(0..list_vars.len());
    list_vars[idx].clone()
}

fn choose_return_var(rng: &mut StdRng, int_vars: &[String], list_vars: &[String]) -> String {
    let total = int_vars.len() + list_vars.len();
    let idx = rng.gen_range(0..total);
    if idx < int_vars.len() {
        int_vars[idx].clone()
    } else {
        list_vars[idx - int_vars.len()].clone()
    }
}

fn next_var(counter: &mut usize) -> String {
    *counter += 1;
    format!("v{counter}")
}

fn next_named_var(counter: &mut usize, prefix: &str) -> String {
    *counter += 1;
    format!("{prefix}_{counter}")
}

fn next_trace_id(counter: &mut i64) -> i64 {
    let trace_id = *counter;
    *counter += 1;
    trace_id
}

fn reserve_trace_block(counter: &mut i64, size: i64) -> i64 {
    let base = *counter;
    *counter += size;
    base
}

fn add_var(kind: ArgKind, name: &str, int_vars: &mut Vec<String>, list_vars: &mut Vec<String>) {
    match kind {
        ArgKind::Int => int_vars.push(name.to_string()),
        ArgKind::ListInt => list_vars.push(name.to_string()),
    }
}

fn render_trace_call(call: &CallSpec) -> String {
    let args = call
        .args
        .iter()
        .map(|(name, expr)| format!("{name}={expr}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}({args}, trace_id={})", call.action, call.trace_id)
}

fn push_line(lines: &mut Vec<String>, indent: usize, line: impl Into<String>) {
    let line = line.into();
    lines.push(format!("{:indent$}{}", "", line, indent = indent * 4));
}

fn emit_random_action(
    rng: &mut StdRng,
    lines: &mut Vec<String>,
    indent: usize,
    int_vars: &mut Vec<String>,
    list_vars: &mut Vec<String>,
    trace_counter: &mut i64,
    var_counter: &mut usize,
) {
    let roll: u8 = rng.gen_range(0..100);
    if roll < 30 {
        let action_a = choose_action(rng, int_vars, list_vars);
        let action_b = choose_action(rng, int_vars, list_vars);
        let (call_a, kind_a) = build_call(
            rng,
            action_a,
            int_vars,
            list_vars,
            trace_counter,
            var_counter,
        );
        let (call_b, kind_b) = build_call(
            rng,
            action_b,
            int_vars,
            list_vars,
            trace_counter,
            var_counter,
        );
        push_line(
            lines,
            indent,
            format!(
                "{}, {} = await asyncio.gather(",
                call_a.output_var, call_b.output_var
            ),
        );
        push_line(
            lines,
            indent + 1,
            format!("{},", render_trace_call(&call_a)),
        );
        push_line(
            lines,
            indent + 1,
            format!("{},", render_trace_call(&call_b)),
        );
        push_line(lines, indent + 1, "return_exceptions=True,");
        push_line(lines, indent, ")");
        add_var(kind_a, &call_a.output_var, int_vars, list_vars);
        add_var(kind_b, &call_b.output_var, int_vars, list_vars);
        return;
    }

    let action = choose_action(rng, int_vars, list_vars);
    let (call, output_kind) =
        build_call(rng, action, int_vars, list_vars, trace_counter, var_counter);
    push_line(
        lines,
        indent,
        format!("{} = await {}", call.output_var, render_trace_call(&call)),
    );
    add_var(output_kind, &call.output_var, int_vars, list_vars);
}

fn emit_if_block(
    rng: &mut StdRng,
    lines: &mut Vec<String>,
    int_vars: &mut Vec<String>,
    list_vars: &mut Vec<String>,
    trace_counter: &mut i64,
    var_counter: &mut usize,
) {
    let output_var = next_var(var_counter);
    let action_true = choose_action_with_output(rng, int_vars, list_vars, ArgKind::Int);
    let action_mid = choose_action_with_output(rng, int_vars, list_vars, ArgKind::Int);
    let action_false = choose_action_with_output(rng, int_vars, list_vars, ArgKind::Int);

    let (call_true, _) = build_call_with_output(
        rng,
        action_true,
        int_vars,
        list_vars,
        trace_counter,
        output_var.clone(),
    );
    let (call_mid, _) = build_call_with_output(
        rng,
        action_mid,
        int_vars,
        list_vars,
        trace_counter,
        output_var.clone(),
    );
    let (call_false, _) = build_call_with_output(
        rng,
        action_false,
        int_vars,
        list_vars,
        trace_counter,
        output_var.clone(),
    );

    push_line(lines, 2, "if seed > 0:");
    push_line(
        lines,
        3,
        format!("{} = await {}", output_var, render_trace_call(&call_true)),
    );
    push_line(lines, 2, "elif seed == 0:");
    push_line(
        lines,
        3,
        format!("{} = await {}", output_var, render_trace_call(&call_mid)),
    );
    push_line(lines, 2, "else:");
    push_line(
        lines,
        3,
        format!("{} = await {}", output_var, render_trace_call(&call_false)),
    );

    add_var(ArgKind::Int, &output_var, int_vars, list_vars);
}

fn emit_for_loop(
    rng: &mut StdRng,
    lines: &mut Vec<String>,
    list_vars: &mut Vec<String>,
    trace_counter: &mut i64,
    var_counter: &mut usize,
) {
    let results_var = "loop_results".to_string();
    let trace_base = reserve_trace_block(trace_counter, 10_000);
    push_line(lines, 2, format!("{results_var} = []"));

    let loop_variant = rng.gen_range(0..=2);
    let mut trace_key = "item".to_string();
    match loop_variant {
        0 => {
            push_line(lines, 2, "for item in items:");
        }
        1 => {
            let idx_var = next_named_var(var_counter, "idx");
            trace_key = idx_var.clone();
            push_line(
                lines,
                2,
                format!("for {idx_var}, item in enumerate(items):"),
            );
        }
        _ => {
            let idx_var = next_named_var(var_counter, "idx");
            trace_key = idx_var.clone();
            push_line(lines, 2, format!("for {idx_var} in range(len(items)):"));
            push_line(lines, 3, format!("item = items[{idx_var}]"));
        }
    }

    let nested = rng.gen_bool(0.45);
    if nested {
        push_line(lines, 3, "inner_sum = 0");
        push_line(lines, 3, "for other in items:");
        let trace_expr = format!("{trace_base} + 5000 + {trace_key} * 100 + other");
        let call = format!("add(a=inner_sum, b=other, trace_id={trace_expr})");
        push_line(lines, 4, format!("inner_sum = await {call}"));
        push_line(lines, 3, format!("{results_var}.append(inner_sum)"));
    } else if rng.gen_bool(0.35) {
        let trace_a = format!("{trace_base} + 6200 + {trace_key}");
        let trace_b = format!("{trace_base} + 6400 + {trace_key}");
        push_line(lines, 3, "proc_a, proc_b = await asyncio.gather(");
        push_line(
            lines,
            4,
            format!("add(a=item, b=factor, trace_id={trace_a}),"),
        );
        push_line(lines, 4, format!("negate(value=item, trace_id={trace_b}),"));
        push_line(lines, 4, "return_exceptions=True,");
        push_line(lines, 3, ")");
        push_line(lines, 3, format!("{results_var}.append(proc_a)"));
        push_line(lines, 3, format!("{results_var}.append(proc_b)"));
    } else {
        if rng.gen_bool(0.4) {
            let trace_pos = format!("{trace_base} + 5200 + {trace_key}");
            let trace_neg = format!("{trace_base} + 5400 + {trace_key}");
            push_line(lines, 3, "if item > 0:");
            push_line(
                lines,
                4,
                format!("processed = await add(a=item, b=factor, trace_id={trace_pos})"),
            );
            push_line(lines, 3, "else:");
            push_line(
                lines,
                4,
                format!("processed = await add(a=item, b=-factor, trace_id={trace_neg})"),
            );
        } else {
            let trace_expr = format!("{trace_base} + 5600 + {trace_key}");
            let call = format!("add(a=item, b=factor, trace_id={trace_expr})");
            push_line(lines, 3, format!("processed = await {call}"));
        }
        push_line(lines, 3, format!("{results_var}.append(processed)"));
    }

    if rng.gen_bool(0.6) {
        let trace_raise = format!("{trace_base} + 9000 + {trace_key}");
        let trace_fallback = format!("{trace_base} + 9500 + {trace_key}");
        push_line(lines, 3, "try:");
        push_line(
            lines,
            4,
            format!(
                "failed = await self.run_action(raise_error(kind=0, trace_id={trace_raise}), retry=RetryPolicy(attempts=1))"
            ),
        );
        push_line(lines, 3, "except ValueError:");
        push_line(
            lines,
            4,
            format!("failed = await add(a=item, b=factor, trace_id={trace_fallback})"),
        );
        push_line(lines, 3, format!("{results_var}.append(failed)"));
    }
    list_vars.push(results_var);
}

fn emit_while_loop(
    rng: &mut StdRng,
    lines: &mut Vec<String>,
    int_vars: &mut Vec<String>,
    list_vars: &mut Vec<String>,
    trace_counter: &mut i64,
    var_counter: &mut usize,
) {
    let results_var = next_named_var(var_counter, "while_results");
    let idx_var = next_named_var(var_counter, "idx");
    let limit_var = next_named_var(var_counter, "limit");
    let trace_base = reserve_trace_block(trace_counter, 15_000);

    push_line(lines, 2, format!("{results_var} = []"));
    push_line(lines, 2, format!("{idx_var} = 0"));
    push_line(lines, 2, format!("{limit_var} = len(items)"));
    push_line(lines, 2, format!("while {idx_var} < {limit_var}:"));
    push_line(lines, 3, format!("current = items[{idx_var}]"));

    if rng.gen_bool(0.4) {
        push_line(lines, 3, "if current < 0:");
        push_line(lines, 4, format!("{idx_var} = {idx_var} + 1"));
        push_line(lines, 4, "continue");
    }

    if rng.gen_bool(0.3) {
        push_line(lines, 3, format!("if {idx_var} == 2:"));
        push_line(lines, 4, "break");
    }

    if rng.gen_bool(0.35) {
        let trace_a = format!("{trace_base} + 12000 + {idx_var}");
        let trace_b = format!("{trace_base} + 12200 + {idx_var}");
        push_line(lines, 3, "proc_a, proc_b = await asyncio.gather(");
        push_line(
            lines,
            4,
            format!("add(a=current, b=factor, trace_id={trace_a}),"),
        );
        push_line(
            lines,
            4,
            format!("negate(value=current, trace_id={trace_b}),"),
        );
        push_line(lines, 4, "return_exceptions=True,");
        push_line(lines, 3, ")");
        push_line(lines, 3, format!("{results_var}.append(proc_a)"));
        push_line(lines, 3, format!("{results_var}.append(proc_b)"));
    } else {
        let trace_expr = format!("{trace_base} + 12400 + {idx_var}");
        let call = format!("add(a=current, b=factor, trace_id={trace_expr})");
        push_line(lines, 3, format!("processed = await {call}"));
        push_line(lines, 3, format!("{results_var}.append(processed)"));
    }

    if rng.gen_bool(0.5) {
        let trace_raise = format!("{trace_base} + 13000 + {idx_var}");
        let trace_fallback = format!("{trace_base} + 13500 + {idx_var}");
        push_line(lines, 3, "try:");
        push_line(
            lines,
            4,
            format!(
                "failed = await self.run_action(raise_error(kind=0, trace_id={trace_raise}), retry=RetryPolicy(attempts=1))"
            ),
        );
        push_line(lines, 3, "except ValueError:");
        push_line(
            lines,
            4,
            format!("failed = await add(a=current, b=factor, trace_id={trace_fallback})"),
        );
        push_line(lines, 3, format!("{results_var}.append(failed)"));
    }

    push_line(lines, 3, format!("{idx_var} = {idx_var} + 1"));

    int_vars.push(idx_var);
    int_vars.push(limit_var);
    list_vars.push(results_var);
}

fn emit_try_except(
    rng: &mut StdRng,
    lines: &mut Vec<String>,
    int_vars: &mut Vec<String>,
    list_vars: &mut Vec<String>,
    trace_counter: &mut i64,
    var_counter: &mut usize,
) {
    let output_var = next_var(var_counter);
    let kind_value = if rng.gen_bool(0.5) { 0 } else { 1 };
    let trace_id = next_trace_id(trace_counter);
    let raise_call = format!(
        "self.run_action(raise_error(kind={kind_value}, trace_id={trace_id}), retry=RetryPolicy(attempts=1))"
    );

    let action_value_error = choose_action_with_output(rng, int_vars, list_vars, ArgKind::Int);
    let action_lookup_error = choose_action_with_output(rng, int_vars, list_vars, ArgKind::Int);
    let action_exception = choose_action_with_output(rng, int_vars, list_vars, ArgKind::Int);
    let (call_value, _) = build_call_with_output(
        rng,
        action_value_error,
        int_vars,
        list_vars,
        trace_counter,
        output_var.clone(),
    );
    let (call_lookup, _) = build_call_with_output(
        rng,
        action_lookup_error,
        int_vars,
        list_vars,
        trace_counter,
        output_var.clone(),
    );
    let (call_exception, _) = build_call_with_output(
        rng,
        action_exception,
        int_vars,
        list_vars,
        trace_counter,
        output_var.clone(),
    );

    push_line(lines, 2, "try:");
    push_line(lines, 3, format!("{} = await {}", output_var, raise_call));
    push_line(lines, 2, "except ValueError:");
    push_line(
        lines,
        3,
        format!("{} = await {}", output_var, render_trace_call(&call_value)),
    );
    push_line(lines, 2, "except LookupError:");
    push_line(
        lines,
        3,
        format!("{} = await {}", output_var, render_trace_call(&call_lookup)),
    );
    push_line(lines, 2, "except Exception:");
    push_line(
        lines,
        3,
        format!(
            "{} = await {}",
            output_var,
            render_trace_call(&call_exception)
        ),
    );

    add_var(ArgKind::Int, &output_var, int_vars, list_vars);
}

fn render_python_module(body_lines: &[String]) -> String {
    let mut source = String::new();
    source.push_str("import asyncio\n");
    source.push_str("from rappel import action, workflow\n");
    source.push_str("from rappel.workflow import Workflow, RetryPolicy\n\n");
    source.push_str("TRACE_LOG = []\n\n");
    source.push_str("def record(trace_id: object, inputs: dict, output: object) -> None:\n");
    source.push_str("    normalized = dict(inputs)\n");
    source.push_str("    normalized[\"trace_id\"] = str(trace_id)\n");
    source.push_str(
        "    TRACE_LOG.append({\"trace_id\": str(trace_id), \"inputs\": normalized, \"output\": output})\n",
    );
    source.push('\n');
    source.push_str("def exception_payload(exc: BaseException) -> dict:\n");
    source.push_str("    return {\n");
    source.push_str("        \"__exception__\": {\n");
    source.push_str("            \"type\": exc.__class__.__name__,\n");
    source.push_str("            \"module\": exc.__class__.__module__,\n");
    source.push_str("            \"message\": str(exc),\n");
    source.push_str("            \"traceback\": \"\",\n");
    source.push_str("            \"values\": {},\n");
    source.push_str("        }\n");
    source.push_str("    }\n\n");
    source.push_str("@action\n");
    source.push_str("async def add(a: int, b: int, trace_id: int) -> int:\n");
    source.push_str("    result = a + b\n");
    source.push_str("    record(trace_id, {\"a\": a, \"b\": b, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str("async def mul(a: int, b: int, trace_id: int) -> int:\n");
    source.push_str("    result = a * b\n");
    source.push_str("    record(trace_id, {\"a\": a, \"b\": b, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str("async def negate(value: int, trace_id: int) -> int:\n");
    source.push_str("    result = -value\n");
    source.push_str("    record(trace_id, {\"value\": value, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str("async def sum_list(items: list[int], trace_id: int) -> int:\n");
    source.push_str("    result = sum(items)\n");
    source.push_str("    record(trace_id, {\"items\": items, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str("async def len_list(items: list[int], trace_id: int) -> int:\n");
    source.push_str("    result = len(items)\n");
    source.push_str("    record(trace_id, {\"items\": items, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str("async def make_list(a: int, b: int, c: int, trace_id: int) -> list[int]:\n");
    source.push_str("    result = [a, b, c]\n");
    source.push_str(
        "    record(trace_id, {\"a\": a, \"b\": b, \"c\": c, \"trace_id\": trace_id}, result)\n",
    );
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str(
        "async def append_list(items: list[int], value: int, trace_id: int) -> list[int]:\n",
    );
    source.push_str("    result = items + [value]\n");
    source.push_str("    record(trace_id, {\"items\": items, \"value\": value, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str(
        "async def scale_list(items: list[int], factor: int, trace_id: int) -> list[int]:\n",
    );
    source.push_str("    result = [item * factor for item in items]\n");
    source.push_str("    record(trace_id, {\"items\": items, \"factor\": factor, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str(
        "async def concat_lists(left: list[int], right: list[int], trace_id: int) -> list[int]:\n",
    );
    source.push_str("    result = left + right\n");
    source.push_str("    record(trace_id, {\"left\": left, \"right\": right, \"trace_id\": trace_id}, result)\n");
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str(
        "async def take_slice(items: list[int], end: int, trace_id: int) -> list[int]:\n",
    );
    source.push_str("    result = items[:end]\n");
    source.push_str(
        "    record(trace_id, {\"items\": items, \"end\": end, \"trace_id\": trace_id}, result)\n",
    );
    source.push_str("    return result\n\n");
    source.push_str("@action\n");
    source.push_str("async def raise_error(kind: int, trace_id: int) -> int:\n");
    source.push_str("    if kind == 0:\n");
    source.push_str("        exc = ValueError(\"boom\")\n");
    source.push_str("    else:\n");
    source.push_str("        exc = KeyError(\"boom\")\n");
    source.push_str(
        "    record(trace_id, {\"kind\": kind, \"trace_id\": trace_id}, exception_payload(exc))\n",
    );
    source.push_str("    raise exc\n\n");
    source.push_str("@workflow\n");
    source.push_str("class FuzzWorkflow(Workflow):\n");
    source
        .push_str("    async def run(self, seed: int, items: list[int], factor: int) -> object:\n");
    for line in body_lines {
        source.push_str(line);
        source.push('\n');
    }
    source
}

async fn run_case(case_idx: usize, case: &FuzzCase, keep: bool) -> Result<()> {
    let tempdir = TempDir::new().context("create tempdir")?;
    let module_path = tempdir.path().join("case.py");
    let inputs_path = tempdir.path().join("inputs.json");
    let trace_out = tempdir.path().join("trace.json");
    let rust_trace_out = tempdir.path().join("rust-trace.json");
    let registration_out = tempdir.path().join("registration.bin");

    std::fs::write(&module_path, &case.python_source).context("write python module")?;
    std::fs::write(&inputs_path, case.inputs.to_string()).context("write inputs")?;

    run_python_oracle(&module_path, &inputs_path, &trace_out, &registration_out)?;
    let oracle_trace = read_trace(&trace_out)?;
    let registration_bytes = std::fs::read(&registration_out).context("read registration bytes")?;

    let rust_trace = run_in_memory(&registration_bytes).await?;
    if let Err(err) = compare_traces(&oracle_trace, &rust_trace) {
        std::fs::write(
            &rust_trace_out,
            serde_json::to_string_pretty(&rust_trace).context("serialize rust trace")?,
        )
        .context("write rust trace")?;
        let preserved = preserve_tempdir(tempdir, keep, case_idx)?;
        if let Some(path) = preserved {
            println!("case {case_idx} artifacts preserved at {}", path.display());
        }
        return Err(err);
    }

    Ok(())
}

fn run_python_oracle(
    module_path: &Path,
    inputs_path: &Path,
    trace_out: &Path,
    registration_out: &Path,
) -> Result<()> {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let python_dir = repo_root.join("python");
    let script_path = python_dir.join("scripts/fuzz_oracle.py");
    let status = Command::new("uv")
        .current_dir(&python_dir)
        .env("PYTHONPATH", python_dir.join("src"))
        .args([
            "run",
            "python",
            script_path.to_str().context("script path")?,
            "--module-path",
            module_path.to_str().context("module path")?,
            "--inputs-path",
            inputs_path.to_str().context("inputs path")?,
            "--trace-out",
            trace_out.to_str().context("trace path")?,
            "--registration-out",
            registration_out.to_str().context("registration path")?,
        ])
        .status()
        .context("run python oracle")?;
    if !status.success() {
        bail!("python oracle failed with status {status}");
    }
    Ok(())
}

fn read_trace(path: &Path) -> Result<OracleTrace> {
    let contents = std::fs::read_to_string(path).context("read trace")?;
    serde_json::from_str(&contents).context("parse trace json")
}

async fn run_in_memory(registration_bytes: &[u8]) -> Result<OracleTrace> {
    let registration =
        proto::WorkflowRegistration::decode(registration_bytes).context("decode registration")?;
    let mut executor = InMemoryWorkflowExecutor::from_registration(registration, true)
        .context("build in-memory executor")?;
    let mut step = executor.start().await.context("start executor")?;
    let mut actions: Vec<ActionTrace> = Vec::new();
    let mut dispatches: VecDeque<proto::ActionDispatch> = VecDeque::new();
    dispatches.extend(step.dispatches);

    while step.completed_payload.is_none() {
        let dispatch = dispatches.pop_front().context("no dispatches available")?;
        let inputs = dispatch
            .kwargs
            .as_ref()
            .map(|kwargs| kwargs.encode_to_vec())
            .and_then(|bytes| workflow_arguments_to_json(&bytes))
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
        let normalized_inputs = normalize_inputs(inputs);
        let trace_id = normalized_inputs
            .get("trace_id")
            .and_then(Value::as_str)
            .context("trace_id missing")?;
        match execute_action(&dispatch.action_name, &normalized_inputs)? {
            ActionOutcome::Ok(output) => {
                actions.push(ActionTrace {
                    trace_id: trace_id.to_string(),
                    inputs: normalized_inputs.clone(),
                    output: output.clone(),
                });
                let payload = build_result_payload(&output);
                let action_result = proto::ActionResult {
                    action_id: dispatch.action_id,
                    success: true,
                    payload: Some(payload),
                    worker_start_ns: 0,
                    worker_end_ns: 0,
                    dispatch_token: None,
                    error_type: None,
                    error_message: None,
                };
                step = executor
                    .handle_action_result(action_result)
                    .await
                    .context("handle action result")?;
                dispatches.extend(step.dispatches);
            }
            ActionOutcome::Err(err) => {
                let output = exception_json(&err);
                actions.push(ActionTrace {
                    trace_id: trace_id.to_string(),
                    inputs: normalized_inputs.clone(),
                    output: output.clone(),
                });
                let payload = build_error_payload(&err);
                let action_result = proto::ActionResult {
                    action_id: dispatch.action_id,
                    success: false,
                    payload: Some(payload),
                    worker_start_ns: 0,
                    worker_end_ns: 0,
                    dispatch_token: None,
                    error_type: Some(err.exc_type.clone()),
                    error_message: Some(err.message.clone()),
                };
                step = executor
                    .handle_action_result(action_result)
                    .await
                    .context("handle action result")?;
                dispatches.extend(step.dispatches);
            }
        }
    }

    let completed_payload = step
        .completed_payload
        .as_ref()
        .context("missing completed payload")?;
    let workflow_json =
        workflow_arguments_to_json(completed_payload).context("decode workflow output")?;
    let workflow_output = workflow_json.get("result").cloned().unwrap_or(Value::Null);

    Ok(OracleTrace {
        actions,
        workflow_output,
    })
}

fn build_result_payload(value: &Value) -> proto::WorkflowArguments {
    proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "result".to_string(),
            value: Some(json_to_workflow_argument_value(value)),
        }],
    }
}

fn build_error_payload(err: &FuzzError) -> proto::WorkflowArguments {
    proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "error".to_string(),
            value: Some(build_exception_value(err)),
        }],
    }
}

fn build_exception_value(err: &FuzzError) -> proto::WorkflowArgumentValue {
    proto::WorkflowArgumentValue {
        kind: Some(proto::workflow_argument_value::Kind::Exception(
            proto::WorkflowErrorValue {
                r#type: err.exc_type.clone(),
                module: err.module.clone(),
                message: err.message.clone(),
                traceback: String::new(),
                values: None,
                type_hierarchy: err.type_hierarchy.clone(),
            },
        )),
    }
}

fn exception_json(err: &FuzzError) -> Value {
    workflow_argument_value_to_json(&build_exception_value(err))
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

fn build_workflow_error_payload(message: &str) -> Vec<u8> {
    let args = proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "error".to_string(),
            value: Some(WorkflowValue::String(message.to_string()).to_proto()),
        }],
    };
    encode_message(&args)
}

fn execute_inline_node(
    instance: &mut InMemoryWorkflowExecutor,
    node_id: &str,
    dag_node: &DAGNode,
) -> Result<Vec<u8>, String> {
    let scope = instance.state.build_scope_for_node(node_id);

    match dag_node.node_type.as_str() {
        "assignment" | "fn_call" => {
            if let Some(assign_expr) = &dag_node.assign_expr {
                match ExpressionEvaluator::evaluate(assign_expr, &scope) {
                    Ok(value) => {
                        let result_bytes = value.to_proto().encode_to_vec();

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
                    .filter(|edge| edge.edge_type == EdgeType::StateMachine)
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
                        let result_bytes = value.to_proto().encode_to_vec();
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

fn execute_action(action: &str, inputs: &Value) -> Result<ActionOutcome> {
    let map = inputs.as_object().context("inputs must be an object")?;
    match action {
        "add" => {
            let a = get_int(map, "a")?;
            let b = get_int(map, "b")?;
            Ok(ActionOutcome::Ok(Value::from(a + b)))
        }
        "mul" => {
            let a = get_int(map, "a")?;
            let b = get_int(map, "b")?;
            Ok(ActionOutcome::Ok(Value::from(a * b)))
        }
        "negate" => {
            let v = get_int(map, "value")?;
            Ok(ActionOutcome::Ok(Value::from(-v)))
        }
        "sum_list" => {
            let items = get_list(map, "items")?;
            Ok(ActionOutcome::Ok(Value::from(items.iter().sum::<i64>())))
        }
        "len_list" => {
            let items = get_list(map, "items")?;
            Ok(ActionOutcome::Ok(Value::from(items.len() as i64)))
        }
        "make_list" => {
            let a = get_int(map, "a")?;
            let b = get_int(map, "b")?;
            let c = get_int(map, "c")?;
            Ok(ActionOutcome::Ok(Value::Array(vec![
                Value::from(a),
                Value::from(b),
                Value::from(c),
            ])))
        }
        "append_list" => {
            let mut items = get_list(map, "items")?;
            let value = get_int(map, "value")?;
            items.push(value);
            Ok(ActionOutcome::Ok(int_list_value(items)))
        }
        "scale_list" => {
            let items = get_list(map, "items")?;
            let factor = get_int(map, "factor")?;
            Ok(ActionOutcome::Ok(int_list_value(
                items.into_iter().map(|v| v * factor).collect(),
            )))
        }
        "concat_lists" => {
            let left = get_list(map, "left")?;
            let right = get_list(map, "right")?;
            let mut combined = left;
            combined.extend(right);
            Ok(ActionOutcome::Ok(int_list_value(combined)))
        }
        "take_slice" => {
            let items = get_list(map, "items")?;
            let end = get_int(map, "end")?;
            let len = items.len() as i64;
            let mut end = if end >= 0 { end } else { len + end };
            if end < 0 {
                end = 0;
            }
            if end > len {
                end = len;
            }
            let slice = items.into_iter().take(end as usize).collect::<Vec<_>>();
            Ok(ActionOutcome::Ok(int_list_value(slice)))
        }
        "raise_error" => {
            let kind = get_int(map, "kind")?;
            Ok(ActionOutcome::Err(build_fuzz_error(kind)))
        }
        other => bail!("unknown action {other}"),
    }
}

fn build_fuzz_error(kind: i64) -> FuzzError {
    if kind == 0 {
        FuzzError {
            exc_type: "ValueError".to_string(),
            message: "boom".to_string(),
            module: "builtins".to_string(),
            type_hierarchy: vec![
                "ValueError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        }
    } else {
        FuzzError {
            exc_type: "KeyError".to_string(),
            message: "'boom'".to_string(),
            module: "builtins".to_string(),
            type_hierarchy: vec![
                "KeyError".to_string(),
                "LookupError".to_string(),
                "Exception".to_string(),
                "BaseException".to_string(),
            ],
        }
    }
}

fn get_int(map: &serde_json::Map<String, Value>, key: &str) -> Result<i64> {
    let value = map.get(key).context("missing int key")?;
    value
        .as_i64()
        .context("expected integer value")
        .with_context(|| format!("key {key}"))
}

fn get_list(map: &serde_json::Map<String, Value>, key: &str) -> Result<Vec<i64>> {
    let value = map.get(key).context("missing list key")?;
    let array = value.as_array().context("expected list value")?;
    array
        .iter()
        .map(|item| item.as_i64().context("expected int in list"))
        .collect::<Result<Vec<_>>>()
        .with_context(|| format!("key {key}"))
}

fn int_list_value(items: Vec<i64>) -> Value {
    Value::Array(items.into_iter().map(Value::from).collect())
}

fn normalize_inputs(mut inputs: Value) -> Value {
    if let Value::Object(ref mut map) = inputs
        && let Some(trace_value) = map.get("trace_id").cloned()
    {
        let trace_str = match trace_value {
            Value::String(s) => s,
            Value::Number(n) => n.to_string(),
            other => other.to_string(),
        };
        map.insert("trace_id".to_string(), Value::String(trace_str));
    }
    inputs
}

fn compare_traces(python: &OracleTrace, rust: &OracleTrace) -> Result<()> {
    if python.workflow_output != rust.workflow_output {
        bail!(
            "workflow output mismatch: python={} rust={}",
            python.workflow_output,
            rust.workflow_output
        );
    }

    let python_map = map_actions(&python.actions)?;
    let rust_map = map_actions(&rust.actions)?;

    if python_map.len() != rust_map.len() {
        bail!(
            "action count mismatch: python={} rust={}",
            python_map.len(),
            rust_map.len()
        );
    }

    for (trace_id, rust_action) in rust_map {
        let python_action = python_map.get(&trace_id).context("missing python trace")?;
        if python_action.inputs != rust_action.inputs {
            bail!(
                "inputs mismatch for {trace_id}: python={} rust={}",
                python_action.inputs,
                rust_action.inputs
            );
        }
        if python_action.output != rust_action.output {
            bail!(
                "output mismatch for {trace_id}: python={} rust={}",
                python_action.output,
                rust_action.output
            );
        }
    }

    Ok(())
}

fn map_actions(actions: &[ActionTrace]) -> Result<HashMap<String, ActionTrace>> {
    let mut map = HashMap::new();
    for action in actions {
        if map
            .insert(action.trace_id.clone(), action.clone())
            .is_some()
        {
            bail!("duplicate trace_id {}", action.trace_id);
        }
    }
    Ok(map)
}

fn preserve_tempdir(tempdir: TempDir, keep: bool, case_idx: usize) -> Result<Option<PathBuf>> {
    if keep {
        return Ok(Some(tempdir.keep()));
    }
    let path = tempdir.keep();
    let preserved = path
        .parent()
        .map(|parent| parent.join(format!("rappel-fuzz-case-{case_idx}")));
    if let Some(dest) = preserved {
        if dest.exists() {
            std::fs::remove_dir_all(&dest).context("remove existing preserve dir")?;
        }
        std::fs::rename(&path, &dest).context("preserve tempdir")?;
        Ok(Some(dest))
    } else {
        Ok(None)
    }
}
