//! Incremental DAG executor for runner state graphs.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap;
use serde_json::Value;
use uuid::Uuid;

use crate::backends::{ActionDone, CoreBackend, GraphUpdate};
use crate::messages::ast as ir;
use crate::observability::obs;
use crate::rappel_core::dag::{
    ActionCallNode, AggregatorNode, DAG, DAGEdge, DagEdgeIndex, EXCEPTION_SCOPE_VAR, EdgeType,
};
use crate::rappel_core::runner::expression_evaluator::is_exception_value;
use crate::rappel_core::runner::state::{
    ActionCallSpec, ExecutionEdge, ExecutionNode, ExecutionNodeType, IndexValue, ListValue,
    LiteralValue, NodeStatus, QueueNodeParams, RunnerState,
};
use crate::rappel_core::runner::value_visitor::ValueExpr;

/// Raised when the runner executor cannot advance safely.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RunnerExecutorError(pub String);

#[derive(Clone, Debug)]
/// Persistence payloads required before dispatching new actions.
/// These need to be written to the backends in order to ensure that we can mark any
/// inflight actions as failed before queuing them up again
pub struct DurableUpdates {
    pub actions_done: Vec<ActionDone>,
    pub graph_updates: Vec<GraphUpdate>,
}

#[derive(Clone, Debug)]
/// Return value for executor steps with newly queued action nodes.
pub struct ExecutorStep {
    pub actions: Vec<ExecutionNode>,
    pub sleep_requests: Vec<SleepRequest>,
    pub updates: Option<DurableUpdates>,
}

#[derive(Clone, Debug)]
/// Sleep requests emitted by the executor with wake-up times.
pub struct SleepRequest {
    pub node_id: Uuid,
    pub wake_at: DateTime<Utc>,
}

/// Action result payloads keyed by execution node id.
type ExecutionResultMap = HashMap<Uuid, Value>;

type FinishedNodeResult = (
    Option<ExecutionNode>,
    Option<Value>,
    Option<ActionDone>,
    Option<ExecutionNode>,
);

struct WalkOutcome {
    actions: Vec<ExecutionNode>,
    sleep_requests: Vec<SleepRequest>,
}

enum TemplateKind {
    SpreadAction(Box<ActionCallNode>),
    Aggregator(String),
    Regular(String),
}

enum SleepDecision {
    Completed,
    Blocked(DateTime<Utc>),
}

#[derive(Clone, Debug)]
struct RetryDecision {
    should_retry: bool,
}

struct RetryPolicyEvaluator<'a> {
    policies: &'a [ir::PolicyBracket],
    exception_name: Option<&'a str>,
}

/// Advance a DAG template using the current runner state and action results.
///
/// The executor treats the DAG as a control-flow template. It queues runtime
/// execution nodes into RunnerState, unrolling loops/spreads into explicit
/// iterations, and stops when it encounters action calls that must be executed
/// by an external worker.
///
/// This serves as a runner supervisor for a single instance that's owned
/// in memory by our logic.
///
/// Each call to increment() starts from finished execution nodes, walks
/// downstream through inline nodes (assignments, branches, joins, etc.), and
/// returns any newly queued action nodes that are now unblocked.
pub struct RunnerExecutor {
    dag: DAG,
    state: RunnerState,
    action_results: ExecutionResultMap,
    backend: Option<Arc<dyn CoreBackend>>,
    template_index: DagEdgeIndex,
    incoming_exec_edges: FxHashMap<Uuid, Vec<ExecutionEdge>>,
    /// Index: template_id -> list of execution node IDs with that template
    template_to_exec_nodes: FxHashMap<String, Vec<Uuid>>,
    /// Cached assignment evaluations for the current increment pass.
    /// Cleared at the start of each increment call.
    eval_cache: RefCell<FxHashMap<(Uuid, String), Value>>,
    instance_id: Option<Uuid>,
}

impl RunnerExecutor {
    pub fn new(
        dag: DAG,
        state: RunnerState,
        // Action results keyed by execution node id.
        action_results: ExecutionResultMap,
        backend: Option<Arc<dyn CoreBackend>>,
    ) -> Self {
        let mut state = state;
        state.dag = Some(dag.clone());
        state.set_link_queued_nodes(false);

        let template_index = dag.edge_index();
        let incoming_exec_edges = Self::build_incoming_exec_edges(&state);
        let template_to_exec_nodes = Self::build_template_to_exec_nodes(&state);

        Self {
            dag,
            state,
            action_results,
            backend,
            template_index,
            incoming_exec_edges,
            template_to_exec_nodes,
            eval_cache: RefCell::new(FxHashMap::default()),
            instance_id: None,
        }
    }

    pub fn state(&self) -> &RunnerState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut RunnerState {
        &mut self.state
    }

    pub fn dag(&self) -> &DAG {
        &self.dag
    }

    pub fn action_results(&self) -> &ExecutionResultMap {
        &self.action_results
    }

    pub fn instance_id(&self) -> Option<Uuid> {
        self.instance_id
    }

    pub fn set_instance_id(&mut self, instance_id: Uuid) {
        self.instance_id = Some(instance_id);
    }

    pub(super) fn eval_cache_get(&self, key: &(Uuid, String)) -> Option<Value> {
        self.eval_cache.borrow().get(key).cloned()
    }

    pub(super) fn eval_cache_insert(&self, key: (Uuid, String), value: Value) {
        self.eval_cache.borrow_mut().insert(key, value);
    }

    /// Store an action result value for a specific execution node id.
    pub fn set_action_result(&mut self, node_id: Uuid, result: Value) {
        self.action_results.insert(node_id, result);
    }

    /// Remove any cached action result for a specific execution node.
    /// Used when re-queuing an action so we don't replay stale results.
    pub fn clear_action_result(&mut self, node_id: Uuid) {
        self.action_results.remove(&node_id);
    }

    /// Fail inflight actions and return any that should be retried.
    ///
    /// Use this after recovering from a crash: running actions are treated as
    /// failed, their attempt counter is incremented if retry policies allow,
    /// and retryable nodes are re-queued for execution.
    pub fn resume(&mut self) -> Result<ExecutorStep, RunnerExecutorError> {
        let mut finished_nodes = Vec::new();
        for (node_id, node) in &self.state.nodes {
            if node.is_action_call() && node.status == NodeStatus::Running {
                finished_nodes.push(*node_id);
                self.action_results
                    .entry(*node_id)
                    .or_insert_with(|| Self::resume_exception_value(*node_id));
            }
        }
        if finished_nodes.is_empty() {
            let updates = self.collect_updates(Vec::new())?;
            return Ok(ExecutorStep {
                actions: Vec::new(),
                sleep_requests: Vec::new(),
                updates,
            });
        }
        self.increment(&finished_nodes)
    }

    /// Advance execution for finished nodes in a single batch.
    ///
    /// Use this when multiple actions complete in the same tick so the graph
    /// update and action inserts are persisted together.
    #[obs]
    pub fn increment(
        &mut self,
        finished_nodes: &[Uuid],
    ) -> Result<ExecutorStep, RunnerExecutorError> {
        self.eval_cache.borrow_mut().clear();
        let mut actions_done: Vec<ActionDone> = Vec::new();
        let mut pending_starts: Vec<(ExecutionNode, Option<Value>)> = Vec::new();
        let mut actions: Vec<ExecutionNode> = Vec::new();
        let mut sleep_requests: Vec<SleepRequest> = Vec::new();
        let mut seen_actions: HashSet<Uuid> = HashSet::new();
        let mut seen_sleep_nodes: HashSet<Uuid> = HashSet::new();

        for finished_node in finished_nodes {
            let node_id = *finished_node;
            let (start, exception_value, done, retry_action) = self.apply_finished_node(node_id)?;
            if let Some(start) = start {
                pending_starts.push((start, exception_value));
            }
            if let Some(done) = done {
                actions_done.push(done);
            }
            if let Some(retry_action) = retry_action
                && seen_actions.insert(retry_action.node_id)
            {
                actions.push(retry_action);
            }
        }

        while let Some((start, exception_value)) = pending_starts.pop() {
            let outcome = self.walk_from(start, exception_value.clone())?;
            for action in outcome.actions {
                // Multiple finished nodes can converge on the same action; avoid duplicates.
                if seen_actions.insert(action.node_id) {
                    actions.push(action);
                }
            }
            for sleep_request in outcome.sleep_requests {
                if seen_sleep_nodes.insert(sleep_request.node_id) {
                    sleep_requests.push(sleep_request);
                }
            }
        }

        let updates = self.collect_updates(actions_done)?;

        // Note: Action timeouts and delayed retries require wall-clock tracking in the run loop.
        // The executor only handles timeout failures once they surface as action results.

        Ok(ExecutorStep {
            actions,
            sleep_requests,
            updates,
        })
    }

    /// Walk downstream from a node, executing inline nodes until blocked by an action node.
    #[obs]
    fn walk_from(
        &mut self,
        node: ExecutionNode,
        exception_value: Option<Value>,
    ) -> Result<WalkOutcome, RunnerExecutorError> {
        let mut pending = vec![(node, exception_value)];
        let mut actions = Vec::new();
        let mut sleep_requests = Vec::new();

        while let Some((current, current_exception)) = pending.pop() {
            // template_id is the DAG node id, not the execution id.
            let template_node_id = match &current.template_id {
                Some(id) => id,
                None => continue,
            };
            let edges = if let Some(template_edges) = self.template_index.outgoing(template_node_id)
            {
                self.select_edges(template_edges, &current, current_exception)?
            } else {
                continue;
            };
            for edge in edges {
                let successors = self.queue_successor(&current, &edge)?;
                for successor in successors {
                    if successor.status == NodeStatus::Completed {
                        continue;
                    }
                    if successor.is_action_call() {
                        actions.push(successor);
                        continue;
                    }
                    if successor.is_sleep() {
                        if !self.inline_ready(&successor) {
                            continue;
                        }
                        match self.handle_sleep_node(&successor)? {
                            SleepDecision::Completed => {
                                pending.push((successor, None));
                            }
                            SleepDecision::Blocked(wake_at) => {
                                sleep_requests.push(SleepRequest {
                                    node_id: successor.node_id,
                                    wake_at,
                                });
                            }
                        }
                        continue;
                    }
                    if !self.inline_ready(&successor) {
                        continue;
                    }
                    self.execute_inline_node(&successor)?;
                    pending.push((successor, None));
                }
            }
        }
        Ok(WalkOutcome {
            actions,
            sleep_requests,
        })
    }

    /// Update state for a finished node and return replay metadata.
    #[obs]
    fn apply_finished_node(
        &mut self,
        node_id: Uuid,
    ) -> Result<FinishedNodeResult, RunnerExecutorError> {
        let mut exception_value: Option<Value> = None;
        let mut action_done: Option<ActionDone> = None;
        let mut retry_action: Option<ExecutionNode> = None;

        let node_is_action = self
            .state
            .nodes
            .get(&node_id)
            .ok_or_else(|| RunnerExecutorError(format!("execution node not found: {node_id}")))?
            .is_action_call();

        if node_is_action {
            let action_value = self.action_results.get(&node_id).cloned().ok_or_else(|| {
                RunnerExecutorError(format!("missing action result for {}", node_id))
            })?;
            if is_exception_value(&action_value) {
                exception_value = Some(action_value);
                if let Some(node) = self.handle_action_failure(node_id, exception_value.as_ref())? {
                    retry_action = Some(node);
                    return Ok((None, None, None, retry_action));
                }
            } else {
                self.state
                    .mark_completed(node_id)
                    .map_err(|err| RunnerExecutorError(err.0))?;
                let assignments = self
                    .state
                    .nodes
                    .get(&node_id)
                    .map(|node| node.assignments.clone())
                    .unwrap_or_default();
                if !assignments.is_empty() {
                    self.state.mark_latest_assignments(node_id, &assignments);
                }
                let attempt = {
                    let node = self.state.nodes.get(&node_id).ok_or_else(|| {
                        RunnerExecutorError(format!("execution node not found: {node_id}"))
                    })?;
                    node.action_attempt
                };
                action_done = Some(ActionDone {
                    execution_id: node_id,
                    attempt,
                    result: action_value,
                });
            }
        } else {
            self.state
                .mark_completed(node_id)
                .map_err(|err| RunnerExecutorError(err.0))?;
        }

        let node =
            self.state.nodes.get(&node_id).cloned().ok_or_else(|| {
                RunnerExecutorError(format!("execution node not found: {node_id}"))
            })?;
        Ok((Some(node), exception_value, action_done, retry_action))
    }

    fn handle_action_failure(
        &mut self,
        node_id: Uuid,
        exception_value: Option<&Value>,
    ) -> Result<Option<ExecutionNode>, RunnerExecutorError> {
        let node =
            self.state.nodes.get(&node_id).ok_or_else(|| {
                RunnerExecutorError(format!("execution node not found: {node_id}"))
            })?;
        let decision = self.retry_decision(node, exception_value)?;
        if decision.should_retry {
            self.state
                .increment_action_attempt(node_id)
                .map_err(|err| RunnerExecutorError(err.0))?;
            let should_queue = !self.state.ready_queue.contains(&node_id);
            if let Some(node) = self.state.nodes.get_mut(&node_id) {
                node.status = NodeStatus::Queued;
            }
            if should_queue {
                self.state.ready_queue.push(node_id);
            }
            let retry_node = self.state.nodes.get(&node_id).cloned().ok_or_else(|| {
                RunnerExecutorError(format!("execution node not found: {node_id}"))
            })?;
            return Ok(Some(retry_node));
        }

        self.state
            .mark_failed(node_id)
            .map_err(|err| RunnerExecutorError(err.0))?;
        if let Some(exception_value) = exception_value {
            let exception_expr = ValueExpr::Literal(LiteralValue {
                value: exception_value.clone(),
            });
            let mut exception_assignment = HashMap::new();
            exception_assignment.insert(EXCEPTION_SCOPE_VAR.to_string(), exception_expr.clone());
            if let Some(node) = self.state.nodes.get_mut(&node_id) {
                node.assignments
                    .insert(EXCEPTION_SCOPE_VAR.to_string(), exception_expr);
            }
            self.state
                .mark_latest_assignments(node_id, &exception_assignment);
        }
        Ok(None)
    }

    fn retry_decision(
        &self,
        node: &ExecutionNode,
        exception_value: Option<&Value>,
    ) -> Result<RetryDecision, RunnerExecutorError> {
        let template_id = match &node.template_id {
            Some(id) => id.clone(),
            None => {
                return Ok(RetryDecision {
                    should_retry: false,
                });
            }
        };
        let template = self.dag.nodes.get(&template_id).ok_or_else(|| {
            RunnerExecutorError(format!("template node not found: {template_id}"))
        })?;
        let action = match template {
            crate::rappel_core::dag::DAGNode::ActionCall(action) => action,
            _ => {
                return Ok(RetryDecision {
                    should_retry: false,
                });
            }
        };
        let exception_name = exception_value.and_then(exception_type);
        let evaluator = RetryPolicyEvaluator {
            policies: &action.policies,
            exception_name,
        };
        Ok(evaluator.decision(node.action_attempt))
    }

    fn resume_exception_value(node_id: Uuid) -> Value {
        let mut map = serde_json::Map::new();
        map.insert(
            "type".to_string(),
            Value::String("ExecutorResume".to_string()),
        );
        map.insert(
            "message".to_string(),
            Value::String(format!(
                "action {node_id} was running during resume and is treated as failed"
            )),
        );
        Value::Object(map)
    }

    /// Select outgoing edges based on guards and exception state.
    fn select_edges(
        &self,
        edges: &[DAGEdge],
        _node: &ExecutionNode,
        exception_value: Option<Value>,
    ) -> Result<Vec<DAGEdge>, RunnerExecutorError> {
        // Fast path: exception handling
        if let Some(exception_value) = exception_value {
            let mut result = Vec::new();
            for edge in edges {
                if edge.exception_types.is_some() && self.exception_matches(edge, &exception_value)
                {
                    result.push(edge.clone());
                }
            }
            return Ok(result);
        }

        // Check if we have any conditional edges (guards or else)
        let has_guards = edges.iter().any(|e| e.guard_expr.is_some());
        let has_else = edges.iter().any(|e| e.is_else);

        if has_guards || has_else {
            // Evaluate guards first
            let mut passed = Vec::new();
            for edge in edges {
                if edge.guard_expr.is_some() && self.evaluate_guard(edge.guard_expr.as_ref())? {
                    passed.push(edge.clone());
                }
            }
            if !passed.is_empty() {
                return Ok(passed);
            }
            // Fall through to else edges
            let mut else_edges = Vec::new();
            for edge in edges {
                if edge.is_else {
                    else_edges.push(edge.clone());
                }
            }
            return Ok(else_edges);
        }

        // Fast path: regular edges (no exceptions, guards, or else)
        let mut result = Vec::with_capacity(edges.len());
        for edge in edges {
            if edge.exception_types.is_none() {
                result.push(edge.clone());
            }
        }
        Ok(result)
    }

    /// Queue successor nodes for a template edge, handling spreads/aggregators.
    fn queue_successor(
        &mut self,
        source: &ExecutionNode,
        edge: &DAGEdge,
    ) -> Result<Vec<ExecutionNode>, RunnerExecutorError> {
        if edge.edge_type != EdgeType::StateMachine {
            return Ok(Vec::new());
        }

        // Extract info from template without holding borrow across mutable calls
        let kind = {
            let template = self.dag.nodes.get(&edge.target).ok_or_else(|| {
                RunnerExecutorError(format!("template node not found: {}", edge.target))
            })?;

            match template {
                crate::rappel_core::dag::DAGNode::ActionCall(action)
                    if action.spread_loop_var.is_some() =>
                {
                    TemplateKind::SpreadAction(Box::new(action.clone()))
                }
                crate::rappel_core::dag::DAGNode::Aggregator(_) => {
                    TemplateKind::Aggregator(template.id().to_string())
                }
                _ => TemplateKind::Regular(template.id().to_string()),
            }
        };

        match kind {
            TemplateKind::SpreadAction(action) => {
                self.expand_spread_action(source, action.as_ref())
            }
            TemplateKind::Aggregator(template_id) => {
                if let Some(existing) = self.find_connected_aggregator(source.node_id, &template_id)
                {
                    return Ok(vec![existing]);
                }
                let agg_node = self.get_or_create_aggregator(&template_id)?;
                self.add_exec_edge(source.node_id, agg_node.node_id);
                Ok(vec![agg_node])
            }
            TemplateKind::Regular(template_id) => {
                let exec_node = self.get_or_create_exec_node(&template_id)?;
                self.add_exec_edge(source.node_id, exec_node.node_id);
                Ok(vec![exec_node])
            }
        }
    }

    /// Unroll a spread action into per-item action nodes and a shared aggregator.
    ///
    /// Example IR:
    /// - results = spread items:item -> @work(item=item)
    ///   Produces one action execution node per element in items and connects
    ///   them to a single aggregator node for results.
    fn expand_spread_action(
        &mut self,
        source: &ExecutionNode,
        template: &ActionCallNode,
    ) -> Result<Vec<ExecutionNode>, RunnerExecutorError> {
        let collection_expr = template.spread_collection_expr.as_ref().ok_or_else(|| {
            RunnerExecutorError("spread action missing collection expression".to_string())
        })?;
        let loop_var = template.spread_loop_var.as_ref().ok_or_else(|| {
            RunnerExecutorError("spread action missing loop variable".to_string())
        })?;
        let elements = self.expand_collection(collection_expr)?;
        let agg_id = template.aggregates_to.as_ref().ok_or_else(|| {
            RunnerExecutorError("spread action missing aggregator link".to_string())
        })?;

        let agg_node = self
            .state
            .queue_template_node(agg_id, None)
            .map_err(|err| RunnerExecutorError(err.0))?;
        if elements.is_empty() {
            return Ok(vec![agg_node]);
        }

        let mut created = Vec::new();
        for (idx, element) in elements.into_iter().enumerate() {
            let exec_node = self.queue_action_from_template(
                template,
                Some(HashMap::from([(loop_var.clone(), element)])),
                Some(idx as i32),
            )?;
            self.add_exec_edge(source.node_id, exec_node.node_id);
            self.add_exec_edge(exec_node.node_id, agg_node.node_id);
            created.push(exec_node);
        }
        Ok(created)
    }

    /// Create an action execution node from a template with optional bindings.
    ///
    /// Example IR:
    /// - @work(value=item) with local_scope{"item": LiteralValue(3)}
    ///   Produces an action node whose kwargs include the literal 3.
    fn queue_action_from_template(
        &mut self,
        template: &ActionCallNode,
        local_scope: Option<HashMap<String, ValueExpr>>,
        iteration_index: Option<i32>,
    ) -> Result<ExecutionNode, RunnerExecutorError> {
        let kwargs = template
            .kwarg_exprs
            .iter()
            .map(|(name, expr)| {
                let value = self
                    .state
                    .expr_to_value(expr, local_scope.as_ref())
                    .map_err(|err| RunnerExecutorError(err.0))?;
                Ok((name.clone(), value))
            })
            .collect::<Result<HashMap<_, _>, RunnerExecutorError>>()?;

        let spec = ActionCallSpec {
            action_name: template.action_name.clone(),
            module_name: template.module_name.clone(),
            kwargs,
        };
        let targets = template
            .targets
            .clone()
            .or_else(|| template.target.clone().map(|target| vec![target]))
            .unwrap_or_default();
        let node = self
            .state
            .queue_node(
                ExecutionNodeType::ActionCall.as_str(),
                &template.label(),
                QueueNodeParams {
                    template_id: Some(template.id.clone()),
                    targets: Some(targets.clone()),
                    action: Some(spec.clone()),
                    ..QueueNodeParams::default()
                },
            )
            .map_err(|err| RunnerExecutorError(err.0))?;
        for value in spec.kwargs.values() {
            self.state.record_data_flow_from_value(node.node_id, value);
        }
        let result = self
            .state
            .assign_action_results(
                &node,
                &template.action_name,
                Some(&targets),
                iteration_index,
                false,
            )
            .map_err(|err| RunnerExecutorError(err.0))?;
        if let Some(node_mut) = self.state.nodes.get_mut(&node.node_id) {
            node_mut.value_expr = Some(ValueExpr::ActionResult(result));
        }
        Ok(node)
    }

    /// Execute a non-action node inline and update assignments/edges.
    fn execute_inline_node(&mut self, node: &ExecutionNode) -> Result<(), RunnerExecutorError> {
        let template_id = node
            .template_id
            .as_ref()
            .ok_or_else(|| RunnerExecutorError("inline node missing template id".to_string()))?;
        let template = self.dag.nodes.get(template_id).ok_or_else(|| {
            RunnerExecutorError(format!("template node not found: {template_id}"))
        })?;

        let aggregator = match template {
            crate::rappel_core::dag::DAGNode::Aggregator(aggregator) => Some(aggregator.clone()),
            _ => None,
        };
        if let Some(aggregator) = aggregator {
            self.apply_aggregator_assignments(node, &aggregator)?;
        }

        self.state
            .mark_completed(node.node_id)
            .map_err(|err| RunnerExecutorError(err.0))
    }

    fn handle_sleep_node(
        &mut self,
        node: &ExecutionNode,
    ) -> Result<SleepDecision, RunnerExecutorError> {
        let now = Utc::now();
        let scheduled_at = self
            .state
            .nodes
            .get(&node.node_id)
            .and_then(|node| node.scheduled_at);
        if let Some(wake_at) = scheduled_at {
            if wake_at <= now {
                self.state
                    .mark_completed(node.node_id)
                    .map_err(|err| RunnerExecutorError(err.0))?;
                return Ok(SleepDecision::Completed);
            }
            return Ok(SleepDecision::Blocked(wake_at));
        }

        let value_expr = self
            .state
            .nodes
            .get(&node.node_id)
            .and_then(|node| node.value_expr.clone())
            .unwrap_or(ValueExpr::Literal(LiteralValue {
                value: Value::Number(0.into()),
            }));
        let materialized = self.state.materialize_value(value_expr);
        let duration_value = self.evaluate_value_expr(&materialized)?;

        let duration_secs = match duration_value {
            Value::Number(value) => value.as_f64().ok_or_else(|| {
                RunnerExecutorError("sleep duration must be a number".to_string())
            })?,
            Value::Null => 0.0,
            _ => {
                return Err(RunnerExecutorError(
                    "sleep duration must be a number".to_string(),
                ));
            }
        };

        if !duration_secs.is_finite() {
            return Err(RunnerExecutorError(
                "sleep duration must be finite".to_string(),
            ));
        }

        if duration_secs <= 0.0 {
            self.state
                .mark_completed(node.node_id)
                .map_err(|err| RunnerExecutorError(err.0))?;
            return Ok(SleepDecision::Completed);
        }

        let duration = Duration::from_secs_f64(duration_secs);
        let chrono_duration = chrono::Duration::from_std(duration)
            .map_err(|_| RunnerExecutorError("sleep duration is out of range".to_string()))?;
        let wake_at = now + chrono_duration;
        self.state
            .set_node_scheduled_at(node.node_id, Some(wake_at))
            .map_err(|err| RunnerExecutorError(err.0))?;
        Ok(SleepDecision::Blocked(wake_at))
    }

    /// Check if an inline node is ready to run based on incoming edges.
    fn inline_ready(&self, node: &ExecutionNode) -> bool {
        if node.status == NodeStatus::Completed {
            return false;
        }
        let incoming = match self.incoming_exec_edges.get(&node.node_id) {
            Some(edges) if !edges.is_empty() => edges,
            _ => return true, // No incoming edges means ready
        };

        let template = match node
            .template_id
            .as_ref()
            .and_then(|id| self.dag.nodes.get(id))
        {
            Some(template) => template,
            None => return false,
        };

        if let crate::rappel_core::dag::DAGNode::Aggregator(_) = template {
            if let Some(required) = self.template_index.incoming(template.id()) {
                let connected = self.connected_template_sources(node.node_id);
                if !required.is_subset(&connected) {
                    return false;
                }
            }
            for edge in incoming {
                if let Some(source) = self.state.nodes.get(&edge.source) {
                    if !matches!(source.status, NodeStatus::Completed | NodeStatus::Failed) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        }

        for edge in incoming {
            if let Some(source) = self.state.nodes.get(&edge.source) {
                if !matches!(source.status, NodeStatus::Completed | NodeStatus::Failed) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    /// Populate aggregated list assignments for a ready aggregator node.
    ///
    /// Example:
    /// - results = spread items: @work(item)
    ///   When all action nodes complete, the aggregator assigns
    ///   results = [ActionResultValue(...), ...].
    fn apply_aggregator_assignments(
        &mut self,
        node: &ExecutionNode,
        template: &AggregatorNode,
    ) -> Result<(), RunnerExecutorError> {
        let targets = template
            .targets
            .clone()
            .or_else(|| template.target.clone().map(|target| vec![target]))
            .unwrap_or_default();
        if targets.len() != 1 {
            return Ok(());
        }

        let incoming_nodes: Vec<ExecutionNode> = self
            .incoming_exec_edges
            .get(&node.node_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|edge| edge.edge_type == EdgeType::StateMachine)
            .filter_map(|edge| self.state.nodes.get(&edge.source).cloned())
            .collect();

        let mut values = Vec::new();
        for source in &incoming_nodes {
            let value_expr = source.value_expr.clone().ok_or_else(|| {
                RunnerExecutorError("aggregator missing source value".to_string())
            })?;
            values.push(value_expr);
        }

        let ordered = self.order_aggregated_values(&incoming_nodes, &values)?;
        let list_value = ValueExpr::List(ListValue { elements: ordered });
        let assignment = HashMap::from([(targets[0].clone(), list_value.clone())]);
        if let Some(node_mut) = self.state.nodes.get_mut(&node.node_id) {
            node_mut.assignments.extend(assignment.clone());
        }
        self.state
            .mark_latest_assignments(node.node_id, &assignment);
        self.state
            .record_data_flow_from_value(node.node_id, &list_value);
        Ok(())
    }

    /// Order aggregator values by spread iteration or parallel index.
    fn order_aggregated_values(
        &self,
        sources: &[ExecutionNode],
        values: &[ValueExpr],
    ) -> Result<Vec<ValueExpr>, RunnerExecutorError> {
        // Order by explicit iteration/parallel indices when available, then fall back to timeline.
        if sources.len() != values.len() {
            return Err(RunnerExecutorError(
                "aggregator sources/value mismatch".to_string(),
            ));
        }
        let timeline_index: HashMap<Uuid, usize> = self
            .state
            .timeline
            .iter()
            .enumerate()
            .map(|(idx, node_id)| (*node_id, idx))
            .collect();
        let mut pairs: Vec<((i32, i32), ValueExpr)> = Vec::with_capacity(values.len());
        for (source, value) in sources.iter().zip(values.iter()) {
            let key = self.aggregated_sort_key(source, value, &timeline_index);
            pairs.push((key, value.clone()));
        }
        pairs.sort_by_key(|item| item.0);
        Ok(pairs.into_iter().map(|(_, value)| value).collect())
    }

    fn aggregated_sort_key(
        &self,
        source: &ExecutionNode,
        value: &ValueExpr,
        timeline_index: &HashMap<Uuid, usize>,
    ) -> (i32, i32) {
        let mut primary = 2;
        let mut secondary = *timeline_index.get(&source.node_id).unwrap_or(&0) as i32;
        if let ValueExpr::ActionResult(action) = value {
            if let Some(iter_idx) = action.iteration_index {
                primary = 0;
                secondary = iter_idx;
            }
        } else if let Some(template_id) = &source.template_id
            && let Some(crate::rappel_core::dag::DAGNode::ActionCall(action)) =
                self.dag.nodes.get(template_id)
            && let Some(idx) = action.parallel_index
        {
            primary = 1;
            secondary = idx;
        }
        (primary, secondary)
    }

    /// Expand a collection expression into element ValueExprs.
    ///
    /// Example IR:
    /// - spread range(3):i -> @work(i)
    ///   Produces [LiteralValue(0), LiteralValue(1), LiteralValue(2)].
    fn expand_collection(
        &mut self,
        expr: &ir::Expr,
    ) -> Result<Vec<ValueExpr>, RunnerExecutorError> {
        let value = Self::expr_to_value(expr)?;
        let value = self.state.materialize_value(value);
        if let ValueExpr::List(list) = value {
            return Ok(list.elements);
        }

        if let ValueExpr::ActionResult(action_value) = value.clone() {
            let action_result = self.resolve_action_result(&action_value)?;
            if let Value::Array(items) = action_result {
                return Ok(items
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| {
                        ValueExpr::Index(IndexValue {
                            object: Box::new(ValueExpr::ActionResult(action_value.clone())),
                            index: Box::new(ValueExpr::Literal(LiteralValue {
                                value: Value::Number((idx as i64).into()),
                            })),
                        })
                    })
                    .collect());
            }
            return Err(RunnerExecutorError(
                "spread collection is not iterable".to_string(),
            ));
        }

        let evaluated = self.evaluate_value_expr(&value)?;
        if let Value::Array(items) = evaluated {
            return Ok(items
                .into_iter()
                .map(|item| ValueExpr::Literal(LiteralValue { value: item }))
                .collect());
        }

        Err(RunnerExecutorError(
            "spread collection is not iterable".to_string(),
        ))
    }

    fn build_incoming_exec_edges(state: &RunnerState) -> FxHashMap<Uuid, Vec<ExecutionEdge>> {
        let mut incoming: FxHashMap<Uuid, Vec<ExecutionEdge>> = FxHashMap::default();
        for edge in &state.edges {
            if edge.edge_type != EdgeType::StateMachine {
                continue;
            }
            incoming.entry(edge.target).or_default().push(edge.clone());
        }
        incoming
    }

    fn build_template_to_exec_nodes(state: &RunnerState) -> FxHashMap<String, Vec<Uuid>> {
        let mut index: FxHashMap<String, Vec<Uuid>> = FxHashMap::default();
        for (node_id, node) in &state.nodes {
            if let Some(template_id) = &node.template_id {
                index.entry(template_id.clone()).or_default().push(*node_id);
            }
        }
        index
    }

    /// Register a new execution node in the template index
    fn register_exec_node(&mut self, template_id: &str, node_id: Uuid) {
        self.template_to_exec_nodes
            .entry(template_id.to_string())
            .or_default()
            .push(node_id);
    }

    fn add_exec_edge(&mut self, source: Uuid, target: Uuid) {
        let edge = ExecutionEdge {
            source,
            target,
            edge_type: EdgeType::StateMachine,
        };
        if self.state.edges.contains(&edge) {
            return;
        }
        self.state.edges.insert(edge.clone());
        self.incoming_exec_edges
            .entry(target)
            .or_default()
            .push(edge);
    }

    fn connected_template_sources(&self, exec_node_id: Uuid) -> HashSet<String> {
        let mut connected = HashSet::new();
        for edge in self
            .incoming_exec_edges
            .get(&exec_node_id)
            .cloned()
            .unwrap_or_default()
        {
            if let Some(source) = self.state.nodes.get(&edge.source)
                && let Some(template_id) = &source.template_id
            {
                connected.insert(template_id.clone());
            }
        }
        connected
    }

    fn find_connected_aggregator(
        &self,
        source_id: Uuid,
        template_id: &str,
    ) -> Option<ExecutionNode> {
        for edge in &self.state.edges {
            if edge.edge_type != EdgeType::StateMachine || edge.source != source_id {
                continue;
            }
            let target = self.state.nodes.get(&edge.target)?;
            if target.template_id.as_deref() == Some(template_id) {
                return Some(target.clone());
            }
        }
        None
    }

    fn get_or_create_aggregator(
        &mut self,
        template_id: &str,
    ) -> Result<ExecutionNode, RunnerExecutorError> {
        let mut candidates: Vec<ExecutionNode> = self
            .state
            .nodes
            .values()
            .filter(|node| {
                node.template_id.as_deref() == Some(template_id)
                    && node.status != NodeStatus::Completed
            })
            .cloned()
            .collect();
        if !candidates.is_empty() {
            let timeline_index: HashMap<Uuid, usize> = self
                .state
                .timeline
                .iter()
                .enumerate()
                .map(|(idx, node_id)| (*node_id, idx))
                .collect();
            candidates.sort_by_key(|node| {
                std::cmp::Reverse(timeline_index.get(&node.node_id).copied().unwrap_or(0))
            });
            return Ok(candidates[0].clone());
        }
        self.state
            .queue_template_node(template_id, None)
            .map_err(|err| RunnerExecutorError(err.0))
    }

    fn get_or_create_exec_node(
        &mut self,
        template_id: &str,
    ) -> Result<ExecutionNode, RunnerExecutorError> {
        // Use the index to find candidate nodes - O(k) where k is nodes for this template
        if let Some(node_ids) = self.template_to_exec_nodes.get(template_id) {
            // Find the most recent non-completed node
            let mut best_node_id: Option<Uuid> = None;
            let mut best_timeline_pos: Option<usize> = None;

            for &node_id in node_ids {
                if let Some(node) = self.state.nodes.get(&node_id)
                    && !matches!(node.status, NodeStatus::Completed | NodeStatus::Failed)
                {
                    let timeline_pos = self.state.timeline.iter().position(|&id| id == node_id);
                    if let Some(pos) = timeline_pos {
                        if best_timeline_pos.is_none() || pos > best_timeline_pos.unwrap() {
                            best_timeline_pos = Some(pos);
                            best_node_id = Some(node_id);
                        }
                    } else if best_node_id.is_none() {
                        best_node_id = Some(node_id);
                    }
                }
            }

            if let Some(node_id) = best_node_id {
                return self
                    .state
                    .nodes
                    .get(&node_id)
                    .cloned()
                    .ok_or_else(|| RunnerExecutorError(format!("node disappeared: {node_id}")));
            }
        }

        // Create new node and register it in the index
        let node = self
            .state
            .queue_template_node(template_id, None)
            .map_err(|err| RunnerExecutorError(err.0))?;
        self.register_exec_node(template_id, node.node_id);
        Ok(node)
    }

    fn collect_updates(
        &mut self,
        actions_done: Vec<ActionDone>,
    ) -> Result<Option<DurableUpdates>, RunnerExecutorError> {
        if self.backend.is_none() {
            return Ok(None);
        }
        let graph_dirty = self.state.consume_graph_dirty_for_durable_execution();
        let mut graph_updates = Vec::new();
        if graph_dirty {
            let instance_id = self.instance_id.ok_or_else(|| {
                RunnerExecutorError("instance_id is required for graph persistence".to_string())
            })?;
            graph_updates.push(GraphUpdate::from_state(instance_id, &self.state));
        }
        let updates = DurableUpdates {
            actions_done,
            graph_updates,
        };
        if updates.actions_done.is_empty() && updates.graph_updates.is_empty() {
            Ok(None)
        } else {
            Ok(Some(updates))
        }
    }
}

impl<'a> RetryPolicyEvaluator<'a> {
    fn decision(&self, attempt: i32) -> RetryDecision {
        let mut max_retries: i32 = 0;
        let mut matched_policy = false;

        for policy in self.policies {
            let Some(ir::policy_bracket::Kind::Retry(retry)) = policy.kind.as_ref() else {
                continue;
            };
            let matches_exception = if retry.exception_types.is_empty() {
                true
            } else if let Some(name) = self.exception_name {
                retry.exception_types.iter().any(|value| value == name)
            } else {
                false
            };
            if !matches_exception {
                continue;
            }
            matched_policy = true;
            max_retries = max_retries.max(retry.max_retries as i32);
        }

        let should_retry = matched_policy && attempt - 1 < max_retries;

        RetryDecision { should_retry }
    }
}

fn exception_type(value: &Value) -> Option<&str> {
    match value {
        Value::Object(map) => map.get("type").and_then(|value| value.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    use crate::messages::ast as ir;
    use crate::rappel_core::dag::{
        ActionCallNode, ActionCallParams, AggregatorNode, AssignmentNode, DAG, DAGEdge,
    };
    use crate::rappel_core::runner::state::{
        ExecutionEdge, ExecutionNode, NodeStatus, RunnerState,
    };

    fn variable(name: &str) -> ir::Expr {
        ir::Expr {
            kind: Some(ir::expr::Kind::Variable(ir::Variable {
                name: name.to_string(),
            })),
            span: None,
        }
    }

    fn literal_int(value: i64) -> ir::Expr {
        ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IntValue(value)),
            })),
            span: None,
        }
    }

    fn binary(left: ir::Expr, op: ir::BinaryOperator, right: ir::Expr) -> ir::Expr {
        ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(left)),
                op: op as i32,
                right: Some(Box::new(right)),
            }))),
            span: None,
        }
    }

    #[derive(Default)]
    struct ActionNodeOptions {
        policies: Vec<ir::PolicyBracket>,
        spread_loop_var: Option<String>,
        spread_collection_expr: Option<ir::Expr>,
        aggregates_to: Option<String>,
    }

    fn action_node(
        node_id: &str,
        action_name: &str,
        kwarg_exprs: HashMap<String, ir::Expr>,
        targets: Vec<String>,
        options: ActionNodeOptions,
    ) -> ActionCallNode {
        let ActionNodeOptions {
            policies,
            spread_loop_var,
            spread_collection_expr,
            aggregates_to,
        } = options;
        ActionCallNode::new(
            node_id,
            action_name,
            ActionCallParams {
                module_name: None,
                kwargs: HashMap::new(),
                kwarg_exprs,
                policies,
                targets: Some(targets),
                target: None,
                parallel_index: None,
                aggregates_to,
                spread_loop_var,
                spread_collection_expr,
                function_name: Some("main".to_string()),
            },
        )
    }

    fn assignment_node(
        node_id: &str,
        targets: Vec<String>,
        assign_expr: ir::Expr,
    ) -> AssignmentNode {
        AssignmentNode::new(
            node_id,
            targets,
            None,
            Some(assign_expr),
            None,
            Some("main".to_string()),
        )
    }

    fn aggregator_node(
        node_id: &str,
        aggregates_from: &str,
        targets: Vec<String>,
    ) -> AggregatorNode {
        AggregatorNode::new(
            node_id,
            aggregates_from,
            Some(targets),
            None,
            "aggregate",
            Some("main".to_string()),
        )
    }

    fn snapshot_state(
        state: &RunnerState,
        action_results: &HashMap<Uuid, Value>,
    ) -> (
        HashMap<Uuid, ExecutionNode>,
        HashSet<ExecutionEdge>,
        HashMap<Uuid, Value>,
    ) {
        (
            state.nodes.clone(),
            state.edges.clone(),
            action_results.clone(),
        )
    }

    fn create_rehydrated_executor(
        dag: &DAG,
        nodes: HashMap<Uuid, ExecutionNode>,
        edges: HashSet<ExecutionEdge>,
        action_results: HashMap<Uuid, Value>,
    ) -> RunnerExecutor {
        let state = RunnerState::new(Some(dag.clone()), Some(nodes), Some(edges), false);
        RunnerExecutor::new(dag.clone(), state, action_results, None)
    }

    fn compare_executor_states(original: &RunnerExecutor, rehydrated: &RunnerExecutor) {
        let orig_state = original.state();
        let rehy_state = rehydrated.state();
        assert_eq!(
            orig_state.nodes.keys().collect::<HashSet<_>>(),
            rehy_state.nodes.keys().collect::<HashSet<_>>(),
        );
        for node_id in orig_state.nodes.keys() {
            let orig_node = orig_state.nodes.get(node_id).unwrap();
            let rehy_node = rehy_state.nodes.get(node_id).unwrap();
            assert_eq!(orig_node.node_type, rehy_node.node_type);
            assert_eq!(orig_node.status, rehy_node.status);
            assert_eq!(orig_node.template_id, rehy_node.template_id);
            assert_eq!(orig_node.targets, rehy_node.targets);
            assert_eq!(orig_node.action_attempt, rehy_node.action_attempt);
        }
        assert_eq!(orig_state.edges, rehy_state.edges);
    }

    #[test]
    fn test_executor_unblocks_downstream_action() {
        let mut dag = DAG::default();

        let action_start = action_node(
            "action_start",
            "fetch",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let assign_node = assignment_node(
            "assign",
            vec!["y".to_string()],
            binary(
                variable("x"),
                ir::BinaryOperator::BinaryOpAdd,
                literal_int(1),
            ),
        );
        let action_next = action_node(
            "action_next",
            "work",
            HashMap::from([("value".to_string(), variable("y"))]),
            vec!["z".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action_start.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::Assignment(
            assign_node.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action_next.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action_start.id.clone(),
            assign_node.id.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            assign_node.id.clone(),
            action_next.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let start_exec = state
            .queue_template_node(&action_start.id, None)
            .expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(start_exec.node_id, Value::Number(10.into()));
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);

        let step = executor
            .increment(&[start_exec.node_id])
            .expect("increment");
        assert_eq!(step.actions.len(), 1);
        assert_eq!(
            step.actions[0].template_id.as_deref(),
            Some(action_next.id.as_str())
        );
    }

    #[test]
    fn test_rehydrate_after_first_action_queued() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "fetch",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let action2 = action_node(
            "action2",
            "process",
            HashMap::from([("value".to_string(), variable("x"))]),
            vec!["y".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action2.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            action2.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");
        let executor = RunnerExecutor::new(dag.clone(), state, HashMap::new(), None);

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        compare_executor_states(&executor, &rehydrated);
        let node = rehydrated.state().nodes.get(&exec1.node_id).expect("node");
        assert_eq!(node.status, NodeStatus::Queued);
    }

    #[test]
    fn test_rehydrate_after_action_completed_and_increment() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "fetch",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let action2 = action_node(
            "action2",
            "process",
            HashMap::from([("value".to_string(), variable("x"))]),
            vec!["y".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action2.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            action2.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(exec1.node_id, Value::Number(42.into()));
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);

        let step = executor.increment(&[exec1.node_id]).expect("increment");
        assert_eq!(step.actions.len(), 1);
        let exec2 = &step.actions[0];
        assert_eq!(exec2.template_id.as_deref(), Some(action2.id.as_str()));

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        let node1 = rehydrated.state().nodes.get(&exec1.node_id).unwrap();
        assert_eq!(node1.status, NodeStatus::Completed);
        let node2 = rehydrated.state().nodes.get(&exec2.node_id).unwrap();
        assert_eq!(node2.status, NodeStatus::Queued);
    }

    #[test]
    fn test_rehydrate_multi_step_chain() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "step1",
            HashMap::new(),
            vec!["a".to_string()],
            ActionNodeOptions::default(),
        );
        let action2 = action_node(
            "action2",
            "step2",
            HashMap::from([("input".to_string(), variable("a"))]),
            vec!["b".to_string()],
            ActionNodeOptions::default(),
        );
        let action3 = action_node(
            "action3",
            "step3",
            HashMap::from([("input".to_string(), variable("b"))]),
            vec!["c".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action2.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action3.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            action2.id.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action2.id.clone(),
            action3.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");
        let mut executor = RunnerExecutor::new(dag.clone(), state, HashMap::new(), None);

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        executor.set_action_result(exec1.node_id, Value::Number(10.into()));
        let step1 = executor.increment(&[exec1.node_id]).expect("increment");
        let exec2 = step1.actions[0].clone();

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        executor.set_action_result(exec2.node_id, Value::Number(20.into()));
        let step2 = executor.increment(&[exec2.node_id]).expect("increment");
        let exec3 = step2.actions[0].clone();

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        executor.set_action_result(exec3.node_id, Value::Number(30.into()));
        let step3 = executor.increment(&[exec3.node_id]).expect("increment");
        assert!(step3.actions.is_empty());

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        for node in rehydrated.state().nodes.values() {
            if node.is_action_call() {
                assert_eq!(node.status, NodeStatus::Completed);
            }
        }
    }

    #[test]
    fn test_rehydrate_with_assignment_node() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "fetch",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let assign = assignment_node(
            "assign",
            vec!["y".to_string()],
            binary(
                variable("x"),
                ir::BinaryOperator::BinaryOpAdd,
                literal_int(1),
            ),
        );
        let action2 = action_node(
            "action2",
            "process",
            HashMap::from([("value".to_string(), variable("y"))]),
            vec!["z".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::Assignment(assign.clone()));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action2.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            assign.id.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            assign.id.clone(),
            action2.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(exec1.node_id, Value::Number(10.into()));
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);

        let step = executor.increment(&[exec1.node_id]).expect("increment");
        assert_eq!(step.actions.len(), 1);
        assert_eq!(
            step.actions[0].template_id.as_deref(),
            Some(action2.id.as_str())
        );

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        let assign_nodes: Vec<_> = rehydrated
            .state()
            .nodes
            .values()
            .filter(|node| node.template_id.as_deref() == Some(&assign.id))
            .collect();
        assert_eq!(assign_nodes.len(), 1);
        assert_eq!(assign_nodes[0].status, NodeStatus::Completed);
        assert!(assign_nodes[0].assignments.contains_key("y"));
    }

    #[test]
    fn test_rehydrate_preserves_action_kwargs() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "compute",
            HashMap::from([
                ("a".to_string(), literal_int(5)),
                (
                    "b".to_string(),
                    ir::Expr {
                        kind: Some(ir::expr::Kind::Literal(ir::Literal {
                            value: Some(ir::literal::Value::StringValue("test".to_string())),
                        })),
                        span: None,
                    },
                ),
            ]),
            vec!["result".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");
        let executor = RunnerExecutor::new(dag.clone(), state, HashMap::new(), None);

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        let orig_node = executor.state().nodes.get(&exec1.node_id).unwrap();
        let rehy_node = rehydrated.state().nodes.get(&exec1.node_id).unwrap();
        assert!(orig_node.action.is_some());
        assert!(rehy_node.action.is_some());
        let orig_action = orig_node.action.as_ref().unwrap();
        let rehy_action = rehy_node.action.as_ref().unwrap();
        assert_eq!(orig_action.action_name, rehy_action.action_name);
        let orig_keys: HashSet<_> = orig_action.kwargs.keys().cloned().collect();
        let rehy_keys: HashSet<_> = rehy_action.kwargs.keys().cloned().collect();
        assert_eq!(orig_keys, rehy_keys);
    }

    #[test]
    fn test_rehydrate_increments_from_same_position() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "first",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let action2 = action_node(
            "action2",
            "second",
            HashMap::new(),
            vec!["y".to_string()],
            ActionNodeOptions::default(),
        );
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action2.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            action2.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(exec1.node_id, Value::Number(100.into()));
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let mut rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        let orig_step = executor.increment(&[exec1.node_id]).expect("increment");
        let rehy_step = rehydrated.increment(&[exec1.node_id]).expect("increment");
        assert_eq!(orig_step.actions.len(), rehy_step.actions.len());
        assert_eq!(
            orig_step.actions[0].template_id,
            rehy_step.actions[0].template_id
        );
    }

    #[test]
    fn test_rehydrate_resume_marks_running_as_retryable() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "work",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions {
                policies: vec![ir::PolicyBracket {
                    kind: Some(ir::policy_bracket::Kind::Retry(ir::RetryPolicy {
                        max_retries: 3,
                        backoff: None,
                        exception_types: Vec::new(),
                    })),
                }],
                ..ActionNodeOptions::default()
            },
        );
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");
        state.mark_running(exec1.node_id).expect("mark running");

        let executor = RunnerExecutor::new(dag.clone(), state, HashMap::new(), None);
        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let mut rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        assert_eq!(
            rehydrated.state().nodes.get(&exec1.node_id).unwrap().status,
            NodeStatus::Running
        );

        let step = rehydrated.resume().expect("resume");
        assert_eq!(step.actions.len(), 1);
        assert_eq!(step.actions[0].node_id, exec1.node_id);
        let node = rehydrated.state().nodes.get(&exec1.node_id).unwrap();
        assert_eq!(node.status, NodeStatus::Queued);
        assert_eq!(node.action_attempt, 2);
    }

    #[test]
    fn test_rehydrate_replay_variables_consistent() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "fetch",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let assign = assignment_node(
            "assign",
            vec!["doubled".to_string()],
            binary(
                variable("x"),
                ir::BinaryOperator::BinaryOpMul,
                literal_int(2),
            ),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::Assignment(assign.clone()));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            assign.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(exec1.node_id, Value::Number(21.into()));
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);
        executor.increment(&[exec1.node_id]).expect("increment");

        let orig_replay = crate::rappel_core::runner::replay_variables(
            executor.state(),
            executor.action_results(),
        )
        .expect("replay");

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        let rehy_replay = crate::rappel_core::runner::replay_variables(
            rehydrated.state(),
            rehydrated.action_results(),
        )
        .expect("replay");
        assert_eq!(orig_replay.variables, rehy_replay.variables);
        assert_eq!(
            rehy_replay.variables.get("doubled"),
            Some(&Value::Number(42.into()))
        );
    }

    #[test]
    fn test_rehydrate_spread_action_with_aggregator() {
        let mut dag = DAG::default();
        let initial_action = action_node(
            "initial",
            "get_items",
            HashMap::new(),
            vec!["items".to_string()],
            ActionNodeOptions::default(),
        );
        let spread_action = action_node(
            "spread_action",
            "process_item",
            HashMap::from([("item".to_string(), variable("item"))]),
            vec!["item_result".to_string()],
            ActionNodeOptions {
                spread_loop_var: Some("item".to_string()),
                spread_collection_expr: Some(variable("items")),
                aggregates_to: Some("aggregator".to_string()),
                ..ActionNodeOptions::default()
            },
        );
        let aggregator =
            aggregator_node("aggregator", "spread_action", vec!["results".to_string()]);

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            initial_action.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            spread_action.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::Aggregator(
            aggregator.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            initial_action.id.clone(),
            spread_action.id.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            spread_action.id.clone(),
            aggregator.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let initial_exec = state
            .queue_template_node(&initial_action.id, None)
            .expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(
            initial_exec.node_id,
            Value::Array(vec![1.into(), 2.into(), 3.into()]),
        );
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);

        let step1 = executor
            .increment(&[initial_exec.node_id])
            .expect("increment");
        assert_eq!(step1.actions.len(), 3);

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        compare_executor_states(&executor, &rehydrated);
        let action_nodes: Vec<_> = executor
            .state()
            .nodes
            .values()
            .filter(|node| {
                node.is_action_call() && node.template_id.as_deref() == Some(&spread_action.id)
            })
            .collect();
        assert_eq!(action_nodes.len(), 3);
        for action_node in action_nodes {
            let rehy_node = rehydrated.state().nodes.get(&action_node.node_id).unwrap();
            assert_eq!(rehy_node.node_type, action_node.node_type);
            assert_eq!(rehy_node.status, action_node.status);
        }
    }

    #[test]
    fn test_rehydrate_full_spread_execution() {
        let mut dag = DAG::default();
        let initial_action = action_node(
            "initial",
            "get_items",
            HashMap::new(),
            vec!["items".to_string()],
            ActionNodeOptions::default(),
        );
        let spread_action = action_node(
            "spread_action",
            "double",
            HashMap::from([("value".to_string(), variable("item"))]),
            vec!["item_result".to_string()],
            ActionNodeOptions {
                spread_loop_var: Some("item".to_string()),
                spread_collection_expr: Some(variable("items")),
                aggregates_to: Some("aggregator".to_string()),
                ..ActionNodeOptions::default()
            },
        );
        let aggregator =
            aggregator_node("aggregator", "spread_action", vec!["results".to_string()]);

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            initial_action.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            spread_action.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::Aggregator(
            aggregator.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            initial_action.id.clone(),
            spread_action.id.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            spread_action.id.clone(),
            aggregator.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let initial_exec = state
            .queue_template_node(&initial_action.id, None)
            .expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(
            initial_exec.node_id,
            Value::Array(vec![10.into(), 20.into()]),
        );
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results.clone(), None);

        let step1 = executor
            .increment(&[initial_exec.node_id])
            .expect("increment");
        let spread_nodes = step1.actions;
        assert_eq!(spread_nodes.len(), 2);

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        for (idx, node) in spread_nodes.iter().enumerate() {
            executor.set_action_result(node.node_id, Value::Number(((idx + 1) * 100).into()));
        }

        let _step2 = executor
            .increment(&spread_nodes.iter().map(|n| n.node_id).collect::<Vec<_>>())
            .expect("increment");

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);
        compare_executor_states(&executor, &rehydrated);

        let agg_nodes: Vec<_> = rehydrated
            .state()
            .nodes
            .values()
            .filter(|node| node.template_id.as_deref() == Some(&aggregator.id))
            .collect();
        assert_eq!(agg_nodes.len(), 1);
        assert_eq!(agg_nodes[0].status, NodeStatus::Completed);
        assert!(agg_nodes[0].assignments.contains_key("results"));
    }

    #[test]
    fn test_rehydrate_timeline_ordering_preserved() {
        let mut dag = DAG::default();
        let mut actions = Vec::new();
        for i in 0..4 {
            actions.push(action_node(
                &format!("action{i}"),
                &format!("step{i}"),
                HashMap::new(),
                vec![format!("x{i}")],
                ActionNodeOptions::default(),
            ));
        }
        for action in &actions {
            dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(action.clone()));
        }
        for i in 0..actions.len() - 1 {
            dag.add_edge(DAGEdge::state_machine(
                actions[i].id.clone(),
                actions[i + 1].id.clone(),
            ));
        }

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let mut exec_nodes: Vec<ExecutionNode> = Vec::new();
        exec_nodes.push(
            state
                .queue_template_node(&actions[0].id, None)
                .expect("queue"),
        );
        let mut executor = RunnerExecutor::new(dag.clone(), state, HashMap::new(), None);

        for i in 0..3 {
            executor.set_action_result(
                exec_nodes.last().unwrap().node_id,
                Value::Number((i * 10).into()),
            );
            let step = executor
                .increment(&[exec_nodes.last().unwrap().node_id])
                .expect("increment");
            if !step.actions.is_empty() {
                exec_nodes.push(step.actions[0].clone());
            }
        }

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        let orig_timeline = executor.state().timeline.clone();
        let rehy_timeline = rehydrated.state().timeline.clone();
        assert_eq!(orig_timeline.len(), rehy_timeline.len());
        assert_eq!(
            orig_timeline.iter().collect::<HashSet<_>>(),
            rehy_timeline.iter().collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_rehydrate_ready_queue_rebuilt() {
        let mut dag = DAG::default();
        let action1 = action_node(
            "action1",
            "first",
            HashMap::new(),
            vec!["x".to_string()],
            ActionNodeOptions::default(),
        );
        let action2 = action_node(
            "action2",
            "second",
            HashMap::new(),
            vec!["y".to_string()],
            ActionNodeOptions::default(),
        );

        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action1.clone(),
        ));
        dag.add_node(crate::rappel_core::dag::DAGNode::ActionCall(
            action2.clone(),
        ));
        dag.add_edge(DAGEdge::state_machine(
            action1.id.clone(),
            action2.id.clone(),
        ));

        let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
        let exec1 = state.queue_template_node(&action1.id, None).expect("queue");

        let mut action_results = HashMap::new();
        action_results.insert(exec1.node_id, Value::Number(50.into()));
        let mut executor = RunnerExecutor::new(dag.clone(), state, action_results, None);
        let step = executor.increment(&[exec1.node_id]).expect("increment");
        let exec2 = step.actions[0].clone();

        let (nodes_snap, edges_snap, results_snap) =
            snapshot_state(executor.state(), executor.action_results());
        let rehydrated = create_rehydrated_executor(&dag, nodes_snap, edges_snap, results_snap);

        let queued_nodes: Vec<_> = rehydrated
            .state()
            .nodes
            .values()
            .filter(|node| node.status == NodeStatus::Queued)
            .collect();
        assert_eq!(queued_nodes.len(), 1);
        assert_eq!(queued_nodes[0].node_id, exec2.node_id);
    }
}
