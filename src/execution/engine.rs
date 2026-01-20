use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result};
use prost::Message;

use crate::completion::{
    CompletionError, CompletionPlan, FrontierCategory, FrontierNode, InlineContext,
    InlineExecutionOptions, analyze_subgraph, execute_inline_subgraph,
    execute_inline_subgraph_with_options,
};
use crate::dag::{DAG, EXCEPTION_SCOPE_VAR, EdgeType};
use crate::dag_state::DAGHelper;
use crate::db::WorkflowInstanceId;
use crate::execution::context::ExecutionContext;
use crate::execution::model::{EnginePlan, EngineStartPlan, ExceptionHandlingOutcome};
use crate::messages::proto;
use crate::value::WorkflowValue;

pub struct CompletionEngine;

impl CompletionEngine {
    pub async fn build_start_plan(ctx: &impl ExecutionContext) -> Result<EngineStartPlan> {
        let dag = ctx.dag();
        let helper = DAGHelper::new(dag);
        let function_names = helper.get_function_names();

        if function_names.is_empty() {
            let plan = CompletionPlan::new(String::new());
            return Ok(EngineStartPlan {
                plan,
                seed_inbox_writes: Vec::new(),
                initial_scope: HashMap::new(),
            });
        }

        let entry_fn = function_names
            .iter()
            .find(|&&name| name == "main")
            .or_else(|| function_names.iter().find(|&&name| !name.starts_with("__")))
            .or(function_names.first())
            .copied()
            .context("no valid entry function found")?;

        let input_node = helper
            .find_input_node(entry_fn)
            .context("input node not found")?;

        let initial_scope = ctx.initial_scope().await?;
        let (scope, inbox_writes) =
            seed_scope_and_inbox(&initial_scope, dag, &input_node.id, ctx.instance_id());

        let subgraph = analyze_subgraph(&input_node.id, dag, &helper);
        let existing_inbox: HashMap<String, HashMap<String, WorkflowValue>> = HashMap::new();
        let inline_ctx = InlineContext {
            initial_scope: &scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let plan = execute_inline_subgraph(
            &input_node.id,
            WorkflowValue::Null,
            inline_ctx,
            &subgraph,
            dag,
            ctx.instance_id(),
        )
        .map_err(|err| match err {
            CompletionError::GuardEvaluationError { node_id, message } => {
                anyhow::anyhow!("guard evaluation failed at {node_id}: {message}")
            }
            CompletionError::WorkflowDeadEnd { guard_errors, .. } => {
                anyhow::anyhow!("guard evaluation failed: {guard_errors:?}")
            }
            other => anyhow::anyhow!(other.to_string()),
        })?;

        Ok(EngineStartPlan {
            plan,
            seed_inbox_writes: inbox_writes,
            initial_scope: scope,
        })
    }

    pub async fn build_success_plan(
        ctx: &impl ExecutionContext,
        node_id: &str,
        payload: &[u8],
    ) -> Result<EnginePlan> {
        let dag = ctx.dag();
        let (base_node_id, mut spread_index) = parse_spread_node_id(node_id);
        if spread_index.is_none() {
            spread_index = parallel_list_index(base_node_id, dag);
        }

        let initial_scope = ctx.initial_scope().await?;
        let result = parse_action_payload(payload, "result");

        let helper = DAGHelper::new(dag);
        let subgraph_start = Instant::now();
        let subgraph = analyze_subgraph(base_node_id, dag, &helper);
        let subgraph_us = subgraph_start.elapsed().as_micros() as u64;

        let inbox_start = Instant::now();
        let existing_inbox = ctx.load_inbox(&subgraph.all_node_ids).await?;
        let inbox_us = inbox_start.elapsed().as_micros() as u64;

        let inline_start = Instant::now();
        let inline_ctx = InlineContext {
            initial_scope: &initial_scope,
            existing_inbox: &existing_inbox,
            spread_index,
        };
        let plan = execute_inline_subgraph(
            base_node_id,
            result,
            inline_ctx,
            &subgraph,
            dag,
            ctx.instance_id(),
        )
        .context("failed to execute inline subgraph")?;
        let inline_us = inline_start.elapsed().as_micros() as u64;

        Ok(EnginePlan {
            plan,
            subgraph_us,
            inbox_us,
            inline_us,
        })
    }

    pub async fn build_barrier_plan(
        ctx: &impl ExecutionContext,
        node_id: &str,
    ) -> Result<CompletionPlan> {
        let dag = ctx.dag();
        let helper = DAGHelper::new(dag);
        let subgraph = analyze_subgraph(node_id, dag, &helper);
        let existing_inbox = ctx.load_inbox(&subgraph.all_node_ids).await?;
        let spread_results = ctx.read_spread_inbox(node_id).await?;
        let aggregated =
            WorkflowValue::List(spread_results.into_iter().map(|(_, value)| value).collect());
        let inline_ctx = InlineContext {
            initial_scope: &ctx.initial_scope().await?,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let plan = execute_inline_subgraph(
            node_id,
            aggregated,
            inline_ctx,
            &subgraph,
            dag,
            ctx.instance_id(),
        )
        .context("failed to execute barrier subgraph")?;

        Ok(plan)
    }

    pub async fn handle_action_failure(
        ctx: &impl ExecutionContext,
        node_id: &str,
        payload: &[u8],
        attempt_number: i32,
        max_retries: i32,
    ) -> Result<ExceptionHandlingOutcome> {
        if attempt_number < max_retries {
            return Ok(ExceptionHandlingOutcome::Retry);
        }

        let dag = ctx.dag();
        let base_node_id = parse_spread_node_id(node_id).0;
        let exception = parse_action_payload(payload, "error");
        let (exception_type, type_hierarchy) = match is_exception_result(&exception) {
            Some(result) => result,
            None => return Ok(ExceptionHandlingOutcome::Unhandled),
        };

        let handler_id =
            get_exception_handlers_from_node(dag, base_node_id, &exception_type, &type_hierarchy)
                .or_else(|| {
                    find_enclosing_fn_call(dag, base_node_id).and_then(|fn_call_id| {
                        get_exception_handlers_from_node(
                            dag,
                            &fn_call_id,
                            &exception_type,
                            &type_hierarchy,
                        )
                    })
                });

        let handler_id = match handler_id {
            Some(handler_id) => handler_id,
            None => return Ok(ExceptionHandlingOutcome::Unhandled),
        };

        let inline_scope =
            build_exception_inline_scope(ctx, base_node_id, &handler_id, &exception).await?;

        let helper = DAGHelper::new(dag);
        let mut subgraph = analyze_subgraph(&handler_id, dag, &helper);
        if let Some(node) = dag.nodes.get(&handler_id) {
            let is_action = node.node_type == "action_call" && !node.is_fn_call;
            let is_output = node.is_output;
            let is_barrier = node.is_aggregator
                || (node.node_type == "join" && node.join_required_count != Some(1));
            if (is_action || is_barrier || is_output)
                && !subgraph
                    .frontier_nodes
                    .iter()
                    .any(|frontier| frontier.node_id == handler_id)
            {
                let category = if is_action {
                    FrontierCategory::Action
                } else if is_barrier {
                    FrontierCategory::Barrier
                } else {
                    FrontierCategory::Output
                };
                subgraph.frontier_nodes.push(FrontierNode {
                    node_id: handler_id.clone(),
                    category,
                    required_count: 1,
                });
            }
        }
        let existing_inbox = ctx.load_inbox(&subgraph.all_node_ids).await?;

        let inline_ctx = InlineContext {
            initial_scope: &inline_scope,
            existing_inbox: &existing_inbox,
            spread_index: None,
        };
        let options = InlineExecutionOptions {
            start_nodes: Some(vec![handler_id.clone()]),
            skip_completed_result_insertion: true,
        };
        let mut plan = execute_inline_subgraph_with_options(
            base_node_id,
            WorkflowValue::Null,
            inline_ctx,
            &subgraph,
            dag,
            ctx.instance_id(),
            options,
        )
        .context("failed to execute exception handler subgraph")?;
        for increment in &mut plan.readiness_increments {
            if increment.node_id == handler_id && increment.required_count == 0 {
                increment.required_count = 1;
            }
        }

        Ok(ExceptionHandlingOutcome::Handled {
            plan: Box::new(plan),
            handler_node_id: handler_id,
        })
    }
}

pub fn parse_spread_node_id(node_id: &str) -> (&str, Option<usize>) {
    if let Some(bracket_pos) = node_id.rfind('[')
        && node_id.ends_with(']')
    {
        let base = &node_id[..bracket_pos];
        let idx_str = &node_id[bracket_pos + 1..node_id.len() - 1];
        if let Ok(idx) = idx_str.parse::<usize>() {
            return (base, Some(idx));
        }
    }
    (node_id, None)
}

pub fn parallel_list_index(node_id: &str, dag: &DAG) -> Option<usize> {
    let node = dag.nodes.get(node_id)?;
    let agg_id = node.aggregates_to.as_ref()?;
    let agg_node = dag.nodes.get(agg_id)?;
    let target_count = agg_node.targets.as_ref().map(|t| t.len()).unwrap_or(0);
    if target_count != 1 {
        return None;
    }

    dag.edges
        .iter()
        .filter(|edge| edge.edge_type == EdgeType::StateMachine)
        .find_map(|edge| {
            if edge.target != node_id {
                return None;
            }
            let condition = edge.condition.as_deref()?;
            let idx_str = condition.strip_prefix("parallel:")?;
            idx_str.parse::<usize>().ok()
        })
}

fn parse_action_payload(payload: &[u8], key: &str) -> WorkflowValue {
    if payload.is_empty() {
        return WorkflowValue::Null;
    }

    match proto::WorkflowArguments::decode(payload) {
        Ok(args) => args
            .arguments
            .iter()
            .find(|arg| arg.key == key)
            .and_then(|arg| arg.value.as_ref())
            .map(WorkflowValue::from_proto)
            .unwrap_or(WorkflowValue::Null),
        Err(_) => serde_json::from_slice::<serde_json::Value>(payload)
            .map(|value| WorkflowValue::from_json(&value))
            .unwrap_or(WorkflowValue::Null),
    }
}

fn is_exception_result(result: &WorkflowValue) -> Option<(String, Vec<String>)> {
    match result {
        WorkflowValue::Exception {
            exc_type,
            type_hierarchy,
            ..
        } => Some((exc_type.clone(), type_hierarchy.clone())),
        _ => None,
    }
}

fn find_enclosing_fn_call(dag: &DAG, node_id: &str) -> Option<String> {
    let node = dag.nodes.get(node_id)?;
    let fn_name = node.function_name.as_ref()?;
    if !fn_name.starts_with("__try_body_") {
        return None;
    }

    for (fn_call_id, fn_call_node) in &dag.nodes {
        if fn_call_node.is_fn_call && fn_call_node.called_function.as_ref() == Some(fn_name) {
            return Some(fn_call_id.clone());
        }
    }
    None
}

fn get_exception_handlers_from_node(
    dag: &DAG,
    node_id: &str,
    exception_type: &str,
    type_hierarchy: &[String],
) -> Option<String> {
    let mut catch_all_handler = None;
    let mut superclass_handler: Option<(String, usize)> = None;

    for edge in &dag.edges {
        if edge.source != node_id {
            continue;
        }
        if let Some(ref exc_types) = edge.exception_types {
            if exc_types.is_empty() {
                if catch_all_handler.is_none() {
                    catch_all_handler = Some(edge.target.clone());
                }
            } else if exc_types.iter().any(|t| t == exception_type) {
                return Some(edge.target.clone());
            } else {
                for handler_type in exc_types {
                    if let Some(pos) = type_hierarchy.iter().position(|t| t == handler_type) {
                        match &superclass_handler {
                            None => {
                                superclass_handler = Some((edge.target.clone(), pos));
                            }
                            Some((_, existing_pos)) if pos < *existing_pos => {
                                superclass_handler = Some((edge.target.clone(), pos));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    superclass_handler
        .map(|(handler, _)| handler)
        .or(catch_all_handler)
}

async fn build_exception_inline_scope(
    ctx: &impl ExecutionContext,
    action_node_id: &str,
    handler_node_id: &str,
    exception: &WorkflowValue,
) -> Result<HashMap<String, WorkflowValue>> {
    let dag = ctx.dag();
    let mut inline_scope = ctx.initial_scope().await?;
    inline_scope.insert(EXCEPTION_SCOPE_VAR.to_string(), exception.clone());

    let action_inbox = ctx.read_inbox(action_node_id).await?;
    for (key, value) in action_inbox {
        inline_scope.insert(key, value);
    }

    let handler_inbox = ctx.read_inbox(handler_node_id).await?;
    for (key, value) in handler_inbox {
        inline_scope.entry(key).or_insert(value);
    }

    for edge in dag.edges.iter() {
        if edge.target == handler_node_id
            && edge.edge_type == EdgeType::DataFlow
            && let Some(var_name) = &edge.variable
            && !inline_scope.contains_key(var_name)
        {
            let source_inbox = ctx.read_inbox(&edge.source).await?;
            if let Some(value) = source_inbox.get(var_name) {
                inline_scope.insert(var_name.clone(), value.clone());
            }
        }
    }

    Ok(inline_scope)
}

fn seed_scope_and_inbox(
    initial_inputs: &HashMap<String, WorkflowValue>,
    dag: &DAG,
    source_node_id: &str,
    instance_id: WorkflowInstanceId,
) -> (
    HashMap<String, WorkflowValue>,
    Vec<crate::completion::InboxWrite>,
) {
    let mut inbox_writes = Vec::new();
    for (var_name, value) in initial_inputs {
        collect_inbox_writes_for_node(
            source_node_id,
            var_name,
            value,
            dag,
            instance_id,
            &mut inbox_writes,
        );
    }
    (initial_inputs.clone(), inbox_writes)
}

fn collect_inbox_writes_for_node(
    source_node_id: &str,
    variable_name: &str,
    value: &WorkflowValue,
    dag: &DAG,
    instance_id: WorkflowInstanceId,
    inbox_writes: &mut Vec<crate::completion::InboxWrite>,
) {
    for edge in dag.edges.iter() {
        if edge.source == source_node_id
            && edge.edge_type == EdgeType::DataFlow
            && edge.variable.as_deref() == Some(variable_name)
        {
            inbox_writes.push(crate::completion::InboxWrite {
                instance_id,
                target_node_id: edge.target.clone(),
                variable_name: variable_name.to_string(),
                value: value.clone(),
                source_node_id: source_node_id.to_string(),
                spread_index: None,
            });
        }
    }
}
