//! Execution-time DAG state with unrolled nodes and symbolic values.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::messages::ast as ir;
use crate::waymark_core::dag::{
    ActionCallNode, AggregatorNode, AssignmentNode, DAG, DAGNode, EdgeType, FnCallNode, JoinNode,
    ReturnNode, SleepNode,
};
use crate::waymark_core::runner::expression_evaluator::is_truthy;
use crate::waymark_core::runner::value_visitor::{
    ValueExpr, collect_value_sources, resolve_value_tree,
};

/// Raised when the runner state cannot be updated safely.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RunnerStateError(pub String);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionCallSpec {
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, ValueExpr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiteralValue {
    pub value: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VariableValue {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionResultValue {
    pub node_id: Uuid,
    pub action_name: String,
    pub iteration_index: Option<i32>,
    pub result_index: Option<i32>,
}

impl ActionResultValue {
    pub fn label(&self) -> String {
        let mut label = self.action_name.clone();
        if let Some(idx) = self.iteration_index {
            label = format!("{label}[{idx}]");
        }
        if let Some(idx) = self.result_index {
            label = format!("{label}[{idx}]");
        }
        label
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinaryOpValue {
    pub left: Box<ValueExpr>,
    pub op: i32,
    pub right: Box<ValueExpr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UnaryOpValue {
    pub op: i32,
    pub operand: Box<ValueExpr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListValue {
    pub elements: Vec<ValueExpr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DictEntryValue {
    pub key: ValueExpr,
    pub value: ValueExpr,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DictValue {
    pub entries: Vec<DictEntryValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexValue {
    pub object: Box<ValueExpr>,
    pub index: Box<ValueExpr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DotValue {
    pub object: Box<ValueExpr>,
    pub attribute: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallValue {
    pub name: String,
    pub args: Vec<ValueExpr>,
    pub kwargs: HashMap<String, ValueExpr>,
    pub global_function: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpreadValue {
    pub collection: Box<ValueExpr>,
    pub loop_var: String,
    pub action: ActionCallSpec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum NodeStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionNodeType {
    Input,
    Output,
    Assignment,
    ActionCall,
    FnCall,
    Parallel,
    Aggregator,
    Branch,
    Join,
    Return,
    Break,
    Continue,
    Sleep,
    Expression,
}

impl ExecutionNodeType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionNodeType::Input => "input",
            ExecutionNodeType::Output => "output",
            ExecutionNodeType::Assignment => "assignment",
            ExecutionNodeType::ActionCall => "action_call",
            ExecutionNodeType::FnCall => "fn_call",
            ExecutionNodeType::Parallel => "parallel",
            ExecutionNodeType::Aggregator => "aggregator",
            ExecutionNodeType::Branch => "branch",
            ExecutionNodeType::Join => "join",
            ExecutionNodeType::Return => "return",
            ExecutionNodeType::Break => "break",
            ExecutionNodeType::Continue => "continue",
            ExecutionNodeType::Sleep => "sleep",
            ExecutionNodeType::Expression => "expression",
        }
    }
}

impl TryFrom<&str> for ExecutionNodeType {
    type Error = RunnerStateError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "input" => Ok(ExecutionNodeType::Input),
            "output" => Ok(ExecutionNodeType::Output),
            "assignment" => Ok(ExecutionNodeType::Assignment),
            "action_call" => Ok(ExecutionNodeType::ActionCall),
            "fn_call" => Ok(ExecutionNodeType::FnCall),
            "parallel" => Ok(ExecutionNodeType::Parallel),
            "aggregator" => Ok(ExecutionNodeType::Aggregator),
            "branch" => Ok(ExecutionNodeType::Branch),
            "join" => Ok(ExecutionNodeType::Join),
            "return" => Ok(ExecutionNodeType::Return),
            "break" => Ok(ExecutionNodeType::Break),
            "continue" => Ok(ExecutionNodeType::Continue),
            "sleep" => Ok(ExecutionNodeType::Sleep),
            "expression" => Ok(ExecutionNodeType::Expression),
            _ => Err(RunnerStateError(format!(
                "unknown execution node type: {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionNode {
    pub node_id: Uuid,
    pub node_type: String,
    pub label: String,
    pub status: NodeStatus,
    pub template_id: Option<String>,
    pub targets: Vec<String>,
    pub action: Option<ActionCallSpec>,
    pub value_expr: Option<ValueExpr>,
    pub assignments: HashMap<String, ValueExpr>,
    pub action_attempt: i32,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub scheduled_at: Option<DateTime<Utc>>,
}

impl ExecutionNode {
    pub fn node_type_enum(&self) -> Result<ExecutionNodeType, RunnerStateError> {
        ExecutionNodeType::try_from(self.node_type.as_str())
    }

    pub fn is_action_call(&self) -> bool {
        matches!(
            ExecutionNodeType::try_from(self.node_type.as_str()),
            Ok(ExecutionNodeType::ActionCall)
        )
    }

    pub fn is_sleep(&self) -> bool {
        matches!(
            ExecutionNodeType::try_from(self.node_type.as_str()),
            Ok(ExecutionNodeType::Sleep)
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct QueueNodeParams {
    pub node_id: Option<Uuid>,
    pub template_id: Option<String>,
    pub targets: Option<Vec<String>>,
    pub action: Option<ActionCallSpec>,
    pub value_expr: Option<ValueExpr>,
    pub scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionEdge {
    pub source: Uuid,
    pub target: Uuid,
    pub edge_type: EdgeType,
}

/// Track queued/executed DAG nodes with an unrolled, symbolic state.
///
/// Design overview:
/// - The runner state is not a variable heap; it is the runtime graph itself,
///   unrolled to the exact nodes that have been queued or executed.
/// - Each execution node stores assignments as symbolic expressions so action
///   results can be replayed later without having the concrete payloads.
/// - Data-flow edges encode which execution node supplies a value to another,
///   while state-machine edges encode execution ordering and control flow. This
///   mirrors how the ground truth IR->DAG functions.
///
/// Expected usage:
/// - Callers queue nodes as the program executes (ie. the DAG template is
///   walked) so loops and spreads expand into explicit iterations.
/// - Callers never mutate variables directly; they record assignments on nodes
///   and let replay walk the graph to reconstruct values.
/// - Persisted state can be rehydrated only with nodes/edges. The constructor will
///   rebuild in-memory cache (like timeline ordering and latest assignment tracking).
///
/// In short, RunnerState is the ground-truth runtime DAG: symbolic assignments
/// plus control/data edges, suitable for replay and visualization.
///
/// Action nodes represent our "frontier" nodes. Because of how we construct the graph and always
/// greedily walk the state until we hit the next actions that are possible to run, we guarantee that
/// leaf nodes are only ever actions.
///
/// Cycle walkthrough (mid-loop example):
/// Suppose we are partway through:
/// - results = []
/// - for item in items:
///     - action_result = @action(item)
///     - results = results + [action_result + 1]
///
/// On a single iteration update:
/// 1) The runner queues an action node for @action(item).
///    - A new execution node is created with a UUID id.
///    - Its assignments map action_result -> ActionResultValue(node_id).
///    - Data-flow edges are added from the node that last defined `item`.
/// 2) The runner queues the assignment node for results update.
///    - The RHS expression is materialized:
///      results + [action_result + 1] becomes a BinaryOpValue whose tree
///      contains the ActionResultValue from step (1), plus a LiteralValue(1).
///    - Data-flow edges are added from the prior results definition node and
///      from the action node created in step (1).
///    - Latest assignment tracking is updated so `results` now points to this
///      new execution node.
///
/// After this iteration, the state graph has explicit nodes for the current
/// action and the results update. Subsequent iterations repeat the same
/// sequence, producing a chain of assignments where replay can reconstruct the
/// incremental `results` value by following data-flow edges.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerState {
    #[serde(skip, default)]
    pub dag: Option<Arc<DAG>>,
    pub nodes: HashMap<Uuid, ExecutionNode>,
    pub edges: HashSet<ExecutionEdge>,
    pub ready_queue: Vec<Uuid>,
    pub timeline: Vec<Uuid>,
    link_queued_nodes: bool,
    latest_assignments: HashMap<String, Uuid>,
    graph_dirty: bool,
}

impl RunnerState {
    pub fn new(
        dag: Option<Arc<DAG>>,
        nodes: Option<HashMap<Uuid, ExecutionNode>>,
        edges: Option<HashSet<ExecutionEdge>>,
        link_queued_nodes: bool,
    ) -> Self {
        let mut state = Self {
            dag,
            nodes: nodes.unwrap_or_default(),
            edges: edges.unwrap_or_default(),
            ready_queue: Vec::new(),
            timeline: Vec::new(),
            link_queued_nodes,
            latest_assignments: HashMap::new(),
            graph_dirty: false,
        };
        if !state.nodes.is_empty() || !state.edges.is_empty() {
            state.rehydrate_state();
        }
        state
    }

    pub(crate) fn set_link_queued_nodes(&mut self, value: bool) {
        self.link_queued_nodes = value;
    }

    pub(crate) fn latest_assignment(&self, name: &str) -> Option<Uuid> {
        self.latest_assignments.get(name).copied()
    }

    /// Queue a runtime node based on the DAG template and apply its effects.
    ///
    /// Use this when stepping through a compiled DAG so the runtime state mirrors
    /// the template node (assignments, action results, and data-flow edges).
    ///
    /// Example IR:
    /// - total = a + b
    ///   When the AssignmentNode template is queued, the execution node records
    ///   the symbolic BinaryOpValue and updates data-flow edges from a/b.
    pub fn queue_template_node(
        &mut self,
        template_id: &str,
        iteration_index: Option<i32>,
    ) -> Result<ExecutionNode, RunnerStateError> {
        let dag = self
            .dag
            .as_ref()
            .ok_or_else(|| RunnerStateError("runner state has no DAG template".to_string()))?;
        let template = dag
            .nodes
            .get(template_id)
            .ok_or_else(|| RunnerStateError(format!("template node not found: {template_id}")))?
            .clone();

        let node_id = Uuid::new_v4();
        let node = ExecutionNode {
            node_id,
            node_type: template.node_type().to_string(),
            label: template.label(),
            status: NodeStatus::Queued,
            template_id: Some(template_id.to_string()),
            targets: self.node_targets(&template),
            action: if let DAGNode::ActionCall(action_node) = &template {
                Some(self.action_spec_from_node(action_node))
            } else {
                None
            },
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: if matches!(template, DAGNode::ActionCall(_)) {
                1
            } else {
                0
            },
            started_at: None,
            completed_at: None,
            scheduled_at: None,
        };

        self.register_node(node.clone())?;
        self.apply_template_node(&node, &template, iteration_index)?;
        Ok(node)
    }

    /// Create a runtime node directly without a DAG template.
    ///
    /// Use this for ad-hoc nodes (tests, synthetic steps) and as a common
    /// builder for higher-level queue helpers like queue_action.
    ///
    /// Example:
    /// - queue_node(node_type="assignment", label="results = []")
    pub fn queue_node(
        &mut self,
        node_type: &str,
        label: &str,
        params: QueueNodeParams,
    ) -> Result<ExecutionNode, RunnerStateError> {
        let node_type_enum = ExecutionNodeType::try_from(node_type)?;
        let QueueNodeParams {
            node_id,
            template_id,
            targets,
            action,
            value_expr,
            scheduled_at,
        } = params;
        let node_id = node_id.unwrap_or_else(Uuid::new_v4);
        let action_attempt = if matches!(node_type_enum, ExecutionNodeType::ActionCall) {
            1
        } else {
            0
        };
        let node = ExecutionNode {
            node_id,
            node_type: node_type.to_string(),
            label: label.to_string(),
            status: NodeStatus::Queued,
            template_id,
            targets: targets.unwrap_or_default(),
            action,
            value_expr,
            assignments: HashMap::new(),
            action_attempt,
            started_at: None,
            completed_at: None,
            scheduled_at,
        };
        self.register_node(node.clone())?;
        Ok(node)
    }

    /// Queue an action call from IR, respecting a local scope for loop vars.
    ///
    /// Use this during IR -> runner-state conversion (including spreads) so
    /// action arguments are converted to symbolic expressions.
    ///
    /// Example IR:
    /// - @double(value=item)
    ///   With local_scope={"item": LiteralValue(2)}, the queued action uses a
    ///   literal argument and links data-flow to the literal's source nodes.
    pub fn queue_action_call(
        &mut self,
        action: &ir::ActionCall,
        targets: Option<Vec<String>>,
        iteration_index: Option<i32>,
        local_scope: Option<&HashMap<String, ValueExpr>>,
    ) -> Result<ActionResultValue, RunnerStateError> {
        let spec = self.action_spec_from_ir(action, local_scope);
        let node = self.queue_node(
            ExecutionNodeType::ActionCall.as_str(),
            &format!("@{}()", spec.action_name),
            QueueNodeParams {
                targets: targets.clone(),
                action: Some(spec.clone()),
                ..QueueNodeParams::default()
            },
        )?;
        for value in spec.kwargs.values() {
            self.record_data_flow_from_value(node.node_id, value);
        }
        let result = self.assign_action_results(
            &node,
            &spec.action_name,
            targets.as_deref(),
            iteration_index,
            true,
        )?;
        if let Some(node_mut) = self.nodes.get_mut(&node.node_id) {
            node_mut.value_expr = Some(ValueExpr::ActionResult(result.clone()));
        }
        Ok(result)
    }

    pub fn mark_running(&mut self, node_id: Uuid) -> Result<(), RunnerStateError> {
        let is_action = {
            let node = self.get_node_mut(node_id)?;
            node.status = NodeStatus::Running;
            let is_action = node.is_action_call();
            if is_action {
                node.started_at = Some(Utc::now());
                node.completed_at = None;
            }
            is_action
        };
        self.ready_queue.retain(|id| id != &node_id);
        if is_action {
            self.mark_graph_dirty();
        }
        Ok(())
    }

    pub fn mark_completed(&mut self, node_id: Uuid) -> Result<(), RunnerStateError> {
        let is_action = {
            let node = self.get_node_mut(node_id)?;
            node.status = NodeStatus::Completed;
            let is_action = node.is_action_call();
            if is_action {
                node.completed_at = Some(Utc::now());
            }
            node.scheduled_at = None;
            is_action
        };
        self.ready_queue.retain(|id| id != &node_id);
        if is_action {
            self.mark_graph_dirty();
        }
        Ok(())
    }

    pub fn mark_failed(&mut self, node_id: Uuid) -> Result<(), RunnerStateError> {
        let is_action = {
            let node = self.get_node_mut(node_id)?;
            node.status = NodeStatus::Failed;
            let is_action = node.is_action_call();
            if is_action {
                node.completed_at = Some(Utc::now());
            }
            node.scheduled_at = None;
            is_action
        };
        self.ready_queue.retain(|id| id != &node_id);
        if is_action {
            self.mark_graph_dirty();
        }
        Ok(())
    }

    pub fn set_node_scheduled_at(
        &mut self,
        node_id: Uuid,
        scheduled_at: Option<DateTime<Utc>>,
    ) -> Result<(), RunnerStateError> {
        let node = self.get_node_mut(node_id)?;
        node.scheduled_at = scheduled_at;
        self.mark_graph_dirty();
        Ok(())
    }

    pub fn increment_action_attempt(&mut self, node_id: Uuid) -> Result<(), RunnerStateError> {
        let node = self.get_node_mut(node_id)?;
        if !node.is_action_call() {
            return Err(RunnerStateError(
                "action attempt increment requires an action_call node".to_string(),
            ));
        }
        node.action_attempt += 1;
        self.mark_graph_dirty();
        Ok(())
    }

    /// Return and clear the graph dirty bit for durable execution.
    ///
    /// Only action nodes and their retry parameters must be persisted; other
    /// nodes are deterministic from the ground-truth DAG definition.
    pub fn consume_graph_dirty_for_durable_execution(&mut self) -> bool {
        let dirty = self.graph_dirty;
        self.graph_dirty = false;
        dirty
    }

    pub fn add_edge(&mut self, source: Uuid, target: Uuid, edge_type: EdgeType) {
        self.register_edge(ExecutionEdge {
            source,
            target,
            edge_type,
        });
    }

    /// Insert a node into the runtime bookkeeping and optional control flow.
    ///
    /// Use this for all queued nodes so the ready queue, timeline, and implicit
    /// state-machine edge ordering remain consistent.
    ///
    /// Example:
    /// - queue node A then node B with link_queued_nodes=True
    ///   This creates a state-machine edge A -> B automatically.
    fn register_node(&mut self, node: ExecutionNode) -> Result<(), RunnerStateError> {
        if self.nodes.contains_key(&node.node_id) {
            return Err(RunnerStateError(format!(
                "execution node already queued: {}",
                node.node_id
            )));
        }
        self.nodes.insert(node.node_id, node.clone());
        self.ready_queue.push(node.node_id);
        if node.is_action_call() {
            self.mark_graph_dirty();
        }
        if self.link_queued_nodes
            && let Some(last) = self.timeline.last()
        {
            self.register_edge(ExecutionEdge {
                source: *last,
                target: node.node_id,
                edge_type: EdgeType::StateMachine,
            });
        }
        self.timeline.push(node.node_id);
        Ok(())
    }

    fn register_edge(&mut self, edge: ExecutionEdge) {
        self.edges.insert(edge);
    }

    fn mark_graph_dirty(&mut self) {
        self.graph_dirty = true;
    }

    /// Rebuild derived structures from persisted nodes and edges.
    ///
    /// Use this when loading a snapshot so timeline ordering, latest assignment
    /// tracking, and ready queue reflect the current node set.
    ///
    /// Example:
    /// - Given nodes {A, B} and edge A -> B, rehydration restores timeline
    ///   [A, B] and marks the latest assignment targets from node B.
    fn rehydrate_state(&mut self) {
        self.timeline = self.build_timeline();
        self.latest_assignments.clear();
        for node_id in &self.timeline {
            if let Some(node) = self.nodes.get(node_id) {
                for target in node.assignments.keys() {
                    self.latest_assignments.insert(target.clone(), *node_id);
                }
            }
        }
        if self.ready_queue.is_empty() {
            self.ready_queue = self
                .timeline
                .iter()
                .filter(|node_id| {
                    self.nodes
                        .get(node_id)
                        .map(|node| node.status == NodeStatus::Queued)
                        .unwrap_or(false)
                })
                .cloned()
                .collect();
        }
    }

    fn build_timeline(&self) -> Vec<Uuid> {
        if self.edges.is_empty() {
            return self.nodes.keys().cloned().collect();
        }
        let mut adjacency: HashMap<Uuid, Vec<Uuid>> = self
            .nodes
            .keys()
            .map(|node_id| (*node_id, Vec::new()))
            .collect();
        let mut in_degree: HashMap<Uuid, usize> =
            self.nodes.keys().map(|node_id| (*node_id, 0)).collect();
        let mut edges: Vec<&ExecutionEdge> = self.edges.iter().collect();
        edges.sort_by_key(|edge| (edge.source, edge.target));
        for edge in edges {
            if edge.edge_type != EdgeType::StateMachine {
                continue;
            }
            if adjacency.contains_key(&edge.source) && adjacency.contains_key(&edge.target) {
                adjacency.entry(edge.source).or_default().push(edge.target);
                *in_degree.entry(edge.target).or_insert(0) += 1;
            }
        }
        let mut queue: Vec<Uuid> = in_degree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(node_id, _)| *node_id)
            .collect();
        queue.sort_by_key(|id| id.to_string());
        let mut order: Vec<Uuid> = Vec::new();
        while !queue.is_empty() {
            let node_id = queue.remove(0);
            order.push(node_id);
            if let Some(neighbors) = adjacency.get(&node_id) {
                let mut sorted = neighbors.clone();
                sorted.sort_by_key(|id| id.to_string());
                for neighbor in sorted {
                    if let Some(degree) = in_degree.get_mut(&neighbor) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(neighbor);
                        }
                    }
                }
                queue.sort_by_key(|id| id.to_string());
            }
        }
        let mut remaining: Vec<Uuid> = self
            .nodes
            .keys()
            .filter(|node_id| !order.contains(node_id))
            .cloned()
            .collect();
        remaining.sort_by_key(|id| id.to_string());
        order.extend(remaining);
        order
    }

    fn get_node_mut(&mut self, node_id: Uuid) -> Result<&mut ExecutionNode, RunnerStateError> {
        self.nodes
            .get_mut(&node_id)
            .ok_or_else(|| RunnerStateError(format!("execution node not found: {node_id}")))
    }

    fn node_targets(&self, node: &DAGNode) -> Vec<String> {
        match node {
            DAGNode::Assignment(AssignmentNode {
                targets, target, ..
            }) => {
                if !targets.is_empty() {
                    return targets.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::ActionCall(ActionCallNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::FnCall(FnCallNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::Join(JoinNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::Aggregator(AggregatorNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            DAGNode::Return(ReturnNode {
                targets, target, ..
            }) => {
                if let Some(list) = targets
                    && !list.is_empty()
                {
                    return list.clone();
                }
                target.clone().map(|item| vec![item]).unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    /// Apply DAG template semantics to a queued execution node.
    ///
    /// Use this right after queue_template_node so assignments, action result
    /// references, and data-flow edges are populated from the template.
    ///
    /// Example IR:
    /// - total = @sum(values=items)
    ///   The ActionCallNode template produces an ActionResultValue and defines
    ///   total via assignments on the execution node.
    fn apply_template_node(
        &mut self,
        exec_node: &ExecutionNode,
        template: &DAGNode,
        iteration_index: Option<i32>,
    ) -> Result<(), RunnerStateError> {
        match template {
            DAGNode::Assignment(AssignmentNode {
                assign_expr: Some(expr),
                ..
            }) => {
                let value_expr = self.expr_to_value(expr, None)?;
                if let Some(node_mut) = self.nodes.get_mut(&exec_node.node_id) {
                    node_mut.value_expr = Some(value_expr.clone());
                }
                self.record_data_flow_from_value(exec_node.node_id, &value_expr);
                let assignments =
                    self.build_assignments(&self.node_targets(template), &value_expr)?;
                if let Some(node) = self.nodes.get_mut(&exec_node.node_id) {
                    node.assignments.extend(assignments.clone());
                }
                self.mark_latest_assignments(exec_node.node_id, &assignments);
                return Ok(());
            }
            DAGNode::ActionCall(ActionCallNode {
                action_name,
                targets,
                target,
                ..
            }) => {
                let kwarg_values = self
                    .nodes
                    .get(&exec_node.node_id)
                    .and_then(|node| node.action.as_ref())
                    .map(|action| action.kwargs.values().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                for expr in &kwarg_values {
                    self.record_data_flow_from_value(exec_node.node_id, expr);
                }
                let targets = targets
                    .clone()
                    .or_else(|| target.clone().map(|item| vec![item]));
                let result = self.assign_action_results(
                    exec_node,
                    action_name,
                    targets.as_deref(),
                    iteration_index,
                    true,
                )?;
                if let Some(node_mut) = self.nodes.get_mut(&exec_node.node_id) {
                    node_mut.value_expr = Some(ValueExpr::ActionResult(result));
                }
                return Ok(());
            }
            DAGNode::Sleep(SleepNode {
                duration_expr: Some(expr),
                ..
            }) => {
                let value_expr = self.expr_to_value(expr, None)?;
                if let Some(node_mut) = self.nodes.get_mut(&exec_node.node_id) {
                    node_mut.value_expr = Some(value_expr.clone());
                }
                self.record_data_flow_from_value(exec_node.node_id, &value_expr);
                return Ok(());
            }
            DAGNode::FnCall(FnCallNode {
                assign_expr: Some(expr),
                ..
            }) => {
                let value_expr = self.expr_to_value(expr, None)?;
                if let Some(node_mut) = self.nodes.get_mut(&exec_node.node_id) {
                    node_mut.value_expr = Some(value_expr.clone());
                }
                self.record_data_flow_from_value(exec_node.node_id, &value_expr);
                let assignments =
                    self.build_assignments(&self.node_targets(template), &value_expr)?;
                if let Some(node) = self.nodes.get_mut(&exec_node.node_id) {
                    node.assignments.extend(assignments.clone());
                }
                self.mark_latest_assignments(exec_node.node_id, &assignments);
                return Ok(());
            }
            DAGNode::Return(ReturnNode {
                assign_expr: Some(expr),
                target,
                ..
            }) => {
                let value_expr = self.expr_to_value(expr, None)?;
                if let Some(node_mut) = self.nodes.get_mut(&exec_node.node_id) {
                    node_mut.value_expr = Some(value_expr.clone());
                }
                self.record_data_flow_from_value(exec_node.node_id, &value_expr);
                let target = target.clone().unwrap_or_else(|| "result".to_string());
                let assignments = self.build_assignments(&[target], &value_expr)?;
                if let Some(node) = self.nodes.get_mut(&exec_node.node_id) {
                    node.assignments.extend(assignments.clone());
                }
                self.mark_latest_assignments(exec_node.node_id, &assignments);
                return Ok(());
            }
            _ => {}
        }
        Ok(())
    }

    /// Create symbolic action results and map them to targets.
    ///
    /// Use this when an action produces one or more results that are assigned
    /// to variables (including tuple unpacking).
    ///
    /// `update_latest` controls whether assigned targets are published into
    /// `latest_assignments` for downstream variable/data-flow resolution.
    ///
    /// Use `update_latest = true` for user-visible assignments so later nodes
    /// can resolve those target names through `latest_assignments`.
    ///
    /// Use `update_latest = false` for internal/synthetic bindings that should
    /// not become globally visible variable definitions. Example: spread action
    /// unroll nodes can bind an internal `_spread_result`, and the aggregator
    /// later publishes the final user target.
    ///
    /// Example IR:
    /// - a, b = @pair()
    ///   This yields ActionResultValue(node_id, result_index=0/1) for a and b.
    pub(crate) fn assign_action_results(
        &mut self,
        node: &ExecutionNode,
        action_name: &str,
        targets: Option<&[String]>,
        iteration_index: Option<i32>,
        update_latest: bool,
    ) -> Result<ActionResultValue, RunnerStateError> {
        let result_ref = ActionResultValue {
            node_id: node.node_id,
            action_name: action_name.to_string(),
            iteration_index,
            result_index: None,
        };
        let targets = targets.unwrap_or(&[]);
        let assignments =
            self.build_assignments(targets, &ValueExpr::ActionResult(result_ref.clone()))?;
        if !assignments.is_empty() {
            if let Some(node) = self.nodes.get_mut(&node.node_id) {
                node.assignments.extend(assignments.clone());
            }
            if update_latest {
                self.mark_latest_assignments(node.node_id, &assignments);
            }
        }
        Ok(result_ref)
    }

    /// Expand an assignment into per-target symbolic values.
    ///
    /// Use this for single-target assignments, tuple unpacking, and action
    /// multi-result binding to keep definitions explicit.
    ///
    /// Example IR:
    /// - a, b = [1, 2]
    ///   Produces {"a": LiteralValue(1), "b": LiteralValue(2)}.
    fn build_assignments(
        &self,
        targets: &[String],
        value: &ValueExpr,
    ) -> Result<HashMap<String, ValueExpr>, RunnerStateError> {
        if targets.is_empty() {
            return Ok(HashMap::new());
        }
        if targets.len() == 1 {
            let mut map = HashMap::new();
            // Keep single-target assignments symbolic to avoid recursively
            // embedding prior values into each update (which can explode
            // persisted runner_instances.state size/depth in loops).
            map.insert(targets[0].clone(), value.clone());
            return Ok(map);
        }
        let value = self.materialize_value(value.clone());

        match value {
            ValueExpr::List(ListValue { elements }) => {
                if elements.len() != targets.len() {
                    return Err(RunnerStateError("tuple unpacking mismatch".to_string()));
                }
                let mut map = HashMap::new();
                for (target, item) in targets.iter().zip(elements.into_iter()) {
                    map.insert(target.clone(), item);
                }
                Ok(map)
            }
            ValueExpr::ActionResult(action_value) => {
                let mut map = HashMap::new();
                for (idx, target) in targets.iter().enumerate() {
                    map.insert(
                        target.clone(),
                        ValueExpr::ActionResult(ActionResultValue {
                            node_id: action_value.node_id,
                            action_name: action_value.action_name.clone(),
                            iteration_index: action_value.iteration_index,
                            result_index: Some(idx as i32),
                        }),
                    );
                }
                Ok(map)
            }
            ValueExpr::FunctionCall(func_value) => {
                let mut map = HashMap::new();
                for (idx, target) in targets.iter().enumerate() {
                    map.insert(
                        target.clone(),
                        ValueExpr::Index(IndexValue {
                            object: Box::new(ValueExpr::FunctionCall(func_value.clone())),
                            index: Box::new(ValueExpr::Literal(LiteralValue {
                                value: serde_json::Value::Number((idx as i64).into()),
                            })),
                        }),
                    );
                }
                Ok(map)
            }
            ValueExpr::Index(index_value) => {
                let mut map = HashMap::new();
                for (idx, target) in targets.iter().enumerate() {
                    map.insert(
                        target.clone(),
                        ValueExpr::Index(IndexValue {
                            object: Box::new(ValueExpr::Index(index_value.clone())),
                            index: Box::new(ValueExpr::Literal(LiteralValue {
                                value: serde_json::Value::Number((idx as i64).into()),
                            })),
                        }),
                    );
                }
                Ok(map)
            }
            _ => Err(RunnerStateError("tuple unpacking mismatch".to_string())),
        }
    }

    /// Inline variable references and apply light constant folding.
    ///
    /// Use this before storing assignments so values are self-contained and
    /// list concatenations are simplified.
    ///
    /// Example IR:
    /// - xs = [1]
    /// - ys = xs + [2]
    ///   Materialization turns ys into ListValue([1, 2]) rather than keeping xs.
    pub(crate) fn materialize_value(&self, value: ValueExpr) -> ValueExpr {
        let resolved = resolve_value_tree(&value, &|name, seen| {
            self.resolve_variable_value(name, seen)
        });
        if let ValueExpr::BinaryOp(BinaryOpValue { left, op, right }) = &resolved
            && ir::BinaryOperator::try_from(*op).ok() == Some(ir::BinaryOperator::BinaryOpAdd)
            && let (ValueExpr::List(left_list), ValueExpr::List(right_list)) = (&**left, &**right)
        {
            let mut elements = left_list.elements.clone();
            elements.extend(right_list.elements.clone());
            return ValueExpr::List(ListValue { elements });
        }
        resolved
    }

    /// Resolve a variable name to its latest symbolic definition.
    ///
    /// Use this when materializing expressions so variables become their
    /// defining expression while guarding against cycles.
    ///
    /// Example IR:
    /// - x = 1
    /// - y = x + 2
    ///   When materializing y, the VariableValue("x") is replaced with the
    ///   LiteralValue(1), yielding a BinaryOpValue(1 + 2) instead of a reference
    ///   to x. This makes downstream replay use the symbolic expression rather
    ///   than requiring a separate variable lookup.
    fn resolve_variable_value(&self, name: &str, seen: &mut HashSet<String>) -> ValueExpr {
        if seen.contains(name) {
            return ValueExpr::Variable(VariableValue {
                name: name.to_string(),
            });
        }
        let node_id = match self.latest_assignments.get(name) {
            Some(node_id) => *node_id,
            None => {
                return ValueExpr::Variable(VariableValue {
                    name: name.to_string(),
                });
            }
        };
        let node = match self.nodes.get(&node_id) {
            Some(node) => node,
            None => {
                return ValueExpr::Variable(VariableValue {
                    name: name.to_string(),
                });
            }
        };
        let assigned = match node.assignments.get(name) {
            Some(value) => value.clone(),
            None => {
                return ValueExpr::Variable(VariableValue {
                    name: name.to_string(),
                });
            }
        };
        // Avoid inlining self-referential updates such as `i = i + 1`.
        // Returning the raw assignment here would inject one "extra step"
        // into materialized consumers (e.g. loop guards), causing off-by-one
        // behavior and deep recursive expression trees.
        if value_expr_contains_variable(&assigned, name) {
            return ValueExpr::Variable(VariableValue {
                name: name.to_string(),
            });
        }
        if let ValueExpr::Variable(var) = &assigned {
            seen.insert(name.to_string());
            return self.resolve_variable_value(&var.name, seen);
        }
        assigned
    }

    pub(crate) fn mark_latest_assignments(
        &mut self,
        node_id: Uuid,
        assignments: &HashMap<String, ValueExpr>,
    ) {
        for target in assignments.keys() {
            self.latest_assignments.insert(target.clone(), node_id);
        }
    }

    /// Add data-flow edges implied by a value expression.
    ///
    /// Use this when a node consumes an expression so upstream dependencies are
    /// encoded in the runtime graph.
    ///
    /// Example IR:
    /// - total = @sum(values)
    ///   A data-flow edge is added from the values assignment node to the action.
    pub(crate) fn record_data_flow_from_value(&mut self, node_id: Uuid, value: &ValueExpr) {
        let source_ids =
            collect_value_sources(value, &|name| self.latest_assignments.get(name).copied());
        self.record_data_flow_edges(node_id, &source_ids);
    }

    /// Register data-flow edges from sources to the given node.
    ///
    /// Example:
    /// - sources {A, B} and node C produce edges A -> C and B -> C.
    fn record_data_flow_edges(&mut self, node_id: Uuid, source_ids: &HashSet<Uuid>) {
        for source_id in source_ids {
            if *source_id == node_id {
                continue;
            }
            self.register_edge(ExecutionEdge {
                source: *source_id,
                target: node_id,
                edge_type: EdgeType::DataFlow,
            });
        }
    }

    /// Convert an IR expression into a symbolic ValueExpr tree.
    ///
    /// Use this when interpreting IR statements or DAG templates into the
    /// runtime state; it queues actions and spreads as needed.
    ///
    /// Example IR:
    /// - total = base + 1
    ///   Produces BinaryOpValue(VariableValue("base"), LiteralValue(1)).
    pub fn expr_to_value(
        &mut self,
        expr: &ir::Expr,
        local_scope: Option<&HashMap<String, ValueExpr>>,
    ) -> Result<ValueExpr, RunnerStateError> {
        match expr.kind.as_ref() {
            Some(ir::expr::Kind::Literal(lit)) => Ok(ValueExpr::Literal(LiteralValue {
                value: literal_value(lit),
            })),
            Some(ir::expr::Kind::Variable(var)) => {
                if let Some(scope) = local_scope
                    && let Some(value) = scope.get(&var.name)
                {
                    return Ok(value.clone());
                }
                Ok(ValueExpr::Variable(VariableValue {
                    name: var.name.clone(),
                }))
            }
            Some(ir::expr::Kind::BinaryOp(op)) => {
                let left = op
                    .left
                    .as_ref()
                    .ok_or_else(|| RunnerStateError("binary op missing left".to_string()))?;
                let right = op
                    .right
                    .as_ref()
                    .ok_or_else(|| RunnerStateError("binary op missing right".to_string()))?;
                let left_value = self.expr_to_value(left, local_scope)?;
                let right_value = self.expr_to_value(right, local_scope)?;
                Ok(self.binary_op_value(op.op, left_value, right_value))
            }
            Some(ir::expr::Kind::UnaryOp(op)) => {
                let operand = op
                    .operand
                    .as_ref()
                    .ok_or_else(|| RunnerStateError("unary op missing operand".to_string()))?;
                let operand_value = self.expr_to_value(operand, local_scope)?;
                Ok(self.unary_op_value(op.op, operand_value))
            }
            Some(ir::expr::Kind::List(list)) => {
                let elements = list
                    .elements
                    .iter()
                    .map(|item| self.expr_to_value(item, local_scope))
                    .collect::<Result<Vec<ValueExpr>, RunnerStateError>>()?;
                Ok(ValueExpr::List(ListValue { elements }))
            }
            Some(ir::expr::Kind::Dict(dict_expr)) => {
                let mut entries = Vec::new();
                for entry in &dict_expr.entries {
                    let key_expr = entry
                        .key
                        .as_ref()
                        .ok_or_else(|| RunnerStateError("dict entry missing key".to_string()))?;
                    let value_expr = entry
                        .value
                        .as_ref()
                        .ok_or_else(|| RunnerStateError("dict entry missing value".to_string()))?;
                    entries.push(DictEntryValue {
                        key: self.expr_to_value(key_expr, local_scope)?,
                        value: self.expr_to_value(value_expr, local_scope)?,
                    });
                }
                Ok(ValueExpr::Dict(DictValue { entries }))
            }
            Some(ir::expr::Kind::Index(index)) => {
                let object = index
                    .object
                    .as_ref()
                    .ok_or_else(|| RunnerStateError("index access missing object".to_string()))?;
                let index_expr = index
                    .index
                    .as_ref()
                    .ok_or_else(|| RunnerStateError("index access missing index".to_string()))?;
                let object_value = self.expr_to_value(object, local_scope)?;
                let index_value = self.expr_to_value(index_expr, local_scope)?;
                Ok(self.index_value(object_value, index_value))
            }
            Some(ir::expr::Kind::Dot(dot)) => {
                let object = dot
                    .object
                    .as_ref()
                    .ok_or_else(|| RunnerStateError("dot access missing object".to_string()))?;
                Ok(ValueExpr::Dot(DotValue {
                    object: Box::new(self.expr_to_value(object, local_scope)?),
                    attribute: dot.attribute.clone(),
                }))
            }
            Some(ir::expr::Kind::FunctionCall(call)) => {
                let args = call
                    .args
                    .iter()
                    .map(|arg| self.expr_to_value(arg, local_scope))
                    .collect::<Result<Vec<ValueExpr>, RunnerStateError>>()?;
                let mut kwargs = HashMap::new();
                for kw in &call.kwargs {
                    if let Some(value) = &kw.value {
                        kwargs.insert(kw.name.clone(), self.expr_to_value(value, local_scope)?);
                    }
                }
                let global_fn = if call.global_function != 0 {
                    Some(call.global_function)
                } else {
                    None
                };
                Ok(ValueExpr::FunctionCall(FunctionCallValue {
                    name: call.name.clone(),
                    args,
                    kwargs,
                    global_function: global_fn,
                }))
            }
            Some(ir::expr::Kind::ActionCall(action)) => {
                let result = self.queue_action_call(action, None, None, local_scope)?;
                Ok(ValueExpr::ActionResult(result))
            }
            Some(ir::expr::Kind::ParallelExpr(parallel)) => {
                let mut calls = Vec::new();
                for call in &parallel.calls {
                    calls.push(self.call_to_value(call, local_scope)?);
                }
                Ok(ValueExpr::List(ListValue { elements: calls }))
            }
            Some(ir::expr::Kind::SpreadExpr(spread)) => self.spread_expr_value(spread, local_scope),
            None => Ok(ValueExpr::Literal(LiteralValue {
                value: serde_json::Value::Null,
            })),
        }
    }

    /// Convert an IR call (action/function) into a ValueExpr.
    ///
    /// Use this for parallel expressions that contain mixed call types.
    ///
    /// Example IR:
    /// - parallel { @double(x), helper(x) }
    ///   Action calls become ActionResultValue nodes; function calls become
    ///   FunctionCallValue expressions.
    fn call_to_value(
        &mut self,
        call: &ir::Call,
        local_scope: Option<&HashMap<String, ValueExpr>>,
    ) -> Result<ValueExpr, RunnerStateError> {
        match call.kind.as_ref() {
            Some(ir::call::Kind::Action(action)) => Ok(ValueExpr::ActionResult(
                self.queue_action_call(action, None, None, local_scope)?,
            )),
            Some(ir::call::Kind::Function(function)) => self.expr_to_value(
                &ir::Expr {
                    kind: Some(ir::expr::Kind::FunctionCall(function.clone())),
                    span: None,
                },
                local_scope,
            ),
            None => Ok(ValueExpr::Literal(LiteralValue {
                value: serde_json::Value::Null,
            })),
        }
    }

    /// Materialize a spread expression into concrete calls or a symbolic spread.
    ///
    /// Use this when converting IR spreads so known list collections unroll to
    /// explicit action calls, while unknown collections stay symbolic.
    ///
    /// Example IR:
    /// - spread [1, 2]:item -> @double(value=item)
    ///   Produces a ListValue of ActionResultValue entries for each item.
    fn spread_expr_value(
        &mut self,
        spread: &ir::SpreadExpr,
        local_scope: Option<&HashMap<String, ValueExpr>>,
    ) -> Result<ValueExpr, RunnerStateError> {
        let collection = self.expr_to_value(
            spread
                .collection
                .as_ref()
                .ok_or_else(|| RunnerStateError("spread collection missing".to_string()))?,
            local_scope,
        )?;
        if let ValueExpr::List(list) = &collection {
            let mut results = Vec::new();
            for (idx, item) in list.elements.iter().enumerate() {
                let mut scope = HashMap::new();
                scope.insert(spread.loop_var.clone(), item.clone());
                let result = self.queue_action_call(
                    spread
                        .action
                        .as_ref()
                        .ok_or_else(|| RunnerStateError("spread action missing".to_string()))?,
                    None,
                    Some(idx as i32),
                    Some(&scope),
                )?;
                results.push(ValueExpr::ActionResult(result));
            }
            return Ok(ValueExpr::List(ListValue { elements: results }));
        }

        let action_spec = self.action_spec_from_ir(
            spread
                .action
                .as_ref()
                .ok_or_else(|| RunnerStateError("spread action missing".to_string()))?,
            None,
        );
        Ok(ValueExpr::Spread(SpreadValue {
            collection: Box::new(collection),
            loop_var: spread.loop_var.clone(),
            action: action_spec,
        }))
    }

    /// Build a binary-op value with simple constant folding.
    ///
    /// Use this when converting IR so literals and list concatenations are
    /// simplified early.
    ///
    /// Example IR:
    /// - total = 1 + 2
    ///   Produces LiteralValue(3) instead of a BinaryOpValue.
    fn binary_op_value(&self, op: i32, left: ValueExpr, right: ValueExpr) -> ValueExpr {
        if ir::BinaryOperator::try_from(op).ok() == Some(ir::BinaryOperator::BinaryOpAdd)
            && let (ValueExpr::List(left_list), ValueExpr::List(right_list)) = (&left, &right)
        {
            let mut elements = left_list.elements.clone();
            elements.extend(right_list.elements.clone());
            return ValueExpr::List(ListValue { elements });
        }
        if let (ValueExpr::Literal(left_val), ValueExpr::Literal(right_val)) = (&left, &right)
            && let Some(folded) = fold_literal_binary(op, &left_val.value, &right_val.value)
        {
            return ValueExpr::Literal(LiteralValue { value: folded });
        }
        ValueExpr::BinaryOp(BinaryOpValue {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    /// Build a unary-op value with constant folding for literals.
    ///
    /// Example IR:
    /// - neg = -1
    ///   Produces LiteralValue(-1) instead of UnaryOpValue.
    fn unary_op_value(&self, op: i32, operand: ValueExpr) -> ValueExpr {
        if let ValueExpr::Literal(lit) = &operand
            && let Some(folded) = fold_literal_unary(op, &lit.value)
        {
            return ValueExpr::Literal(LiteralValue { value: folded });
        }
        ValueExpr::UnaryOp(UnaryOpValue {
            op,
            operand: Box::new(operand),
        })
    }

    /// Build an index value, folding list literals when possible.
    ///
    /// Example IR:
    /// - first = [10, 20][0]
    ///   Produces LiteralValue(10) when the list is fully literal.
    fn index_value(&self, object: ValueExpr, index: ValueExpr) -> ValueExpr {
        if let (ValueExpr::List(list), ValueExpr::Literal(idx)) = (&object, &index)
            && let Some(idx) = idx.value.as_i64()
            && idx >= 0
            && (idx as usize) < list.elements.len()
        {
            return list.elements[idx as usize].clone();
        }
        ValueExpr::Index(IndexValue {
            object: Box::new(object),
            index: Box::new(index),
        })
    }

    /// Extract an action call spec from a DAG node.
    ///
    /// Use this when queueing nodes from the DAG template.
    ///
    /// Example:
    /// - ActionCallNode(action_name="double", kwargs={"value": "$x"})
    ///   Produces ActionCallSpec(action_name="double", kwargs={"value": VariableValue("x")}).
    fn action_spec_from_node(&mut self, node: &ActionCallNode) -> ActionCallSpec {
        let kwargs = node
            .kwarg_exprs
            .iter()
            .map(|(name, expr)| (name.clone(), self.expr_to_value(expr, None).unwrap()))
            .collect();
        ActionCallSpec {
            action_name: node.action_name.clone(),
            module_name: node.module_name.clone(),
            kwargs,
        }
    }

    /// Extract an action call spec from IR, applying local scope bindings.
    ///
    /// Example IR:
    /// - @double(value=item) with local_scope["item"]=LiteralValue(2)
    ///   Produces kwargs {"value": LiteralValue(2)}.
    fn action_spec_from_ir(
        &mut self,
        action: &ir::ActionCall,
        local_scope: Option<&HashMap<String, ValueExpr>>,
    ) -> ActionCallSpec {
        let kwargs = action
            .kwargs
            .iter()
            .filter_map(|kw| kw.value.as_ref().map(|value| (kw.name.clone(), value)))
            .map(|(name, value)| (name, self.expr_to_value(value, local_scope).unwrap()))
            .collect();
        ActionCallSpec {
            action_name: action.action_name.clone(),
            module_name: action.module_name.clone(),
            kwargs,
        }
    }

    /// Queue an action call from raw parameters and return a symbolic result.
    ///
    /// Use this when constructing runner state programmatically without IR
    /// objects, while still wiring data-flow edges and assignments.
    ///
    /// Example:
    /// - queue_action("double", targets=["out"], kwargs={"value": LiteralValue(2)})
    ///   Defines out via an ActionResultValue and records data-flow from the literal.
    pub fn queue_action(
        &mut self,
        action_name: &str,
        targets: Option<Vec<String>>,
        kwargs: Option<HashMap<String, ValueExpr>>,
        module_name: Option<String>,
        iteration_index: Option<i32>,
    ) -> Result<ActionResultValue, RunnerStateError> {
        let spec = ActionCallSpec {
            action_name: action_name.to_string(),
            module_name,
            kwargs: kwargs.unwrap_or_default(),
        };
        let node = self.queue_node(
            ExecutionNodeType::ActionCall.as_str(),
            &format!("@{}()", spec.action_name),
            QueueNodeParams {
                targets: targets.clone(),
                action: Some(spec.clone()),
                ..QueueNodeParams::default()
            },
        )?;
        for value in spec.kwargs.values() {
            self.record_data_flow_from_value(node.node_id, value);
        }
        let result = self.assign_action_results(
            &node,
            &spec.action_name,
            targets.as_deref(),
            iteration_index,
            true,
        )?;
        if let Some(node) = self.nodes.get_mut(&node.node_id) {
            node.value_expr = Some(ValueExpr::ActionResult(result.clone()));
        }
        Ok(result)
    }

    /// Record an IR assignment as a runtime node with symbolic values.
    ///
    /// Use this when interpreting IR statements into the unrolled runtime graph.
    ///
    /// Example IR:
    /// - results = []
    ///   Produces an assignment node with targets ["results"] and a ListValue([]).
    pub fn record_assignment(
        &mut self,
        targets: Vec<String>,
        expr: &ir::Expr,
        node_id: Option<Uuid>,
        label: Option<String>,
    ) -> Result<ExecutionNode, RunnerStateError> {
        let value_expr = self.expr_to_value(expr, None)?;
        self.record_assignment_value(targets, value_expr, node_id, label)
    }

    /// Record a symbolic assignment node and update data-flow/definitions.
    ///
    /// Use this for assignments created programmatically after ValueExpr
    /// construction (tests or state rewrites).
    ///
    /// Example:
    /// - record_assignment_value(targets=["x"], value_expr=LiteralValue(1))
    ///   Creates an assignment node with x bound to LiteralValue(1).
    pub fn record_assignment_value(
        &mut self,
        targets: Vec<String>,
        value_expr: ValueExpr,
        node_id: Option<Uuid>,
        label: Option<String>,
    ) -> Result<ExecutionNode, RunnerStateError> {
        let exec_node_id = node_id.unwrap_or_else(Uuid::new_v4);
        let node = self.queue_node(
            "assignment",
            label.as_deref().unwrap_or("assignment"),
            QueueNodeParams {
                node_id: Some(exec_node_id),
                targets: Some(targets.clone()),
                value_expr: Some(value_expr.clone()),
                ..QueueNodeParams::default()
            },
        )?;
        self.record_data_flow_from_value(exec_node_id, &value_expr);
        let assignments = self.build_assignments(&targets, &value_expr)?;
        if let Some(node_mut) = self.nodes.get_mut(&node.node_id) {
            node_mut.assignments.extend(assignments.clone());
        }
        self.mark_latest_assignments(node.node_id, &assignments);
        Ok(node)
    }
}

/// Render a ValueExpr to a python-like string for debugging/visualization.
///
/// Example:
/// - BinaryOpValue(VariableValue("a"), +, LiteralValue(1)) -> "a + 1"
pub fn format_value(expr: &ValueExpr) -> String {
    format_value_inner(expr, 0)
}

/// Recursive ValueExpr formatter with operator precedence handling.
///
/// Example:
/// - (a + b) * c renders with parentheses when needed.
fn format_value_inner(expr: &ValueExpr, parent_prec: i32) -> String {
    match expr {
        ValueExpr::Literal(lit) => format_literal(&lit.value),
        ValueExpr::Variable(var) => var.name.clone(),
        ValueExpr::ActionResult(value) => value.label(),
        ValueExpr::BinaryOp(value) => {
            let (op_str, prec) = binary_operator(value.op);
            let left = format_value_inner(&value.left, prec);
            let right = format_value_inner(&value.right, prec + 1);
            let rendered = format!("{left} {op_str} {right}");
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::UnaryOp(value) => {
            let (op_str, prec) = unary_operator(value.op);
            let operand = format_value_inner(&value.operand, prec);
            let rendered = format!("{op_str}{operand}");
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::List(value) => {
            let items: Vec<String> = value
                .elements
                .iter()
                .map(|item| format_value_inner(item, 0))
                .collect();
            format!("[{}]", items.join(", "))
        }
        ValueExpr::Dict(value) => {
            let entries: Vec<String> = value
                .entries
                .iter()
                .map(|entry| {
                    format!(
                        "{}: {}",
                        format_value_inner(&entry.key, 0),
                        format_value_inner(&entry.value, 0)
                    )
                })
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        ValueExpr::Index(value) => {
            let prec = precedence("index");
            let obj = format_value_inner(&value.object, prec);
            let idx = format_value_inner(&value.index, 0);
            let rendered = format!("{obj}[{idx}]");
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::Dot(value) => {
            let prec = precedence("dot");
            let obj = format_value_inner(&value.object, prec);
            let rendered = format!("{obj}.{}", value.attribute);
            if prec < parent_prec {
                format!("({rendered})")
            } else {
                rendered
            }
        }
        ValueExpr::FunctionCall(value) => {
            let mut args: Vec<String> = value
                .args
                .iter()
                .map(|arg| format_value_inner(arg, 0))
                .collect();
            for (name, val) in &value.kwargs {
                args.push(format!("{name}={}", format_value_inner(val, 0)));
            }
            format!("{}({})", value.name, args.join(", "))
        }
        ValueExpr::Spread(value) => {
            let collection = format_value_inner(&value.collection, 0);
            let mut args: Vec<String> = Vec::new();
            for (name, val) in &value.action.kwargs {
                args.push(format!("{name}={}", format_value_inner(val, 0)));
            }
            let call = format!("@{}({})", value.action.action_name, args.join(", "));
            format!("spread {collection}:{} -> {call}", value.loop_var)
        }
    }
}

fn value_expr_contains_variable(expr: &ValueExpr, name: &str) -> bool {
    match expr {
        ValueExpr::Variable(var) => var.name == name,
        ValueExpr::BinaryOp(value) => {
            value_expr_contains_variable(&value.left, name)
                || value_expr_contains_variable(&value.right, name)
        }
        ValueExpr::UnaryOp(value) => value_expr_contains_variable(&value.operand, name),
        ValueExpr::List(value) => value
            .elements
            .iter()
            .any(|item| value_expr_contains_variable(item, name)),
        ValueExpr::Dict(value) => value.entries.iter().any(|entry| {
            value_expr_contains_variable(&entry.key, name)
                || value_expr_contains_variable(&entry.value, name)
        }),
        ValueExpr::Index(value) => {
            value_expr_contains_variable(&value.object, name)
                || value_expr_contains_variable(&value.index, name)
        }
        ValueExpr::Dot(value) => value_expr_contains_variable(&value.object, name),
        ValueExpr::FunctionCall(value) => {
            value
                .args
                .iter()
                .any(|arg| value_expr_contains_variable(arg, name))
                || value
                    .kwargs
                    .values()
                    .any(|kwarg| value_expr_contains_variable(kwarg, name))
        }
        ValueExpr::Spread(value) => {
            value_expr_contains_variable(&value.collection, name)
                || value
                    .action
                    .kwargs
                    .values()
                    .any(|kwarg| value_expr_contains_variable(kwarg, name))
        }
        ValueExpr::Literal(_) | ValueExpr::ActionResult(_) => false,
    }
}

/// Map binary operator enums to (symbol, precedence) for formatting.
fn binary_operator(op: i32) -> (&'static str, i32) {
    match ir::BinaryOperator::try_from(op).ok() {
        Some(ir::BinaryOperator::BinaryOpOr) => ("or", 10),
        Some(ir::BinaryOperator::BinaryOpAnd) => ("and", 20),
        Some(ir::BinaryOperator::BinaryOpEq) => ("==", 30),
        Some(ir::BinaryOperator::BinaryOpNe) => ("!=", 30),
        Some(ir::BinaryOperator::BinaryOpLt) => ("<", 30),
        Some(ir::BinaryOperator::BinaryOpLe) => ("<=", 30),
        Some(ir::BinaryOperator::BinaryOpGt) => (">", 30),
        Some(ir::BinaryOperator::BinaryOpGe) => (">=", 30),
        Some(ir::BinaryOperator::BinaryOpIn) => ("in", 30),
        Some(ir::BinaryOperator::BinaryOpNotIn) => ("not in", 30),
        Some(ir::BinaryOperator::BinaryOpAdd) => ("+", 40),
        Some(ir::BinaryOperator::BinaryOpSub) => ("-", 40),
        Some(ir::BinaryOperator::BinaryOpMul) => ("*", 50),
        Some(ir::BinaryOperator::BinaryOpDiv) => ("/", 50),
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => ("//", 50),
        Some(ir::BinaryOperator::BinaryOpMod) => ("%", 50),
        _ => ("?", 0),
    }
}

/// Map unary operator enums to (symbol, precedence) for formatting.
fn unary_operator(op: i32) -> (&'static str, i32) {
    match ir::UnaryOperator::try_from(op).ok() {
        Some(ir::UnaryOperator::UnaryOpNeg) => ("-", 60),
        Some(ir::UnaryOperator::UnaryOpNot) => ("not ", 60),
        _ => ("?", 0),
    }
}

/// Return precedence for non-operator constructs like index/dot.
fn precedence(kind: &str) -> i32 {
    match kind {
        "index" | "dot" => 80,
        _ => 0,
    }
}

/// Format Python literals as source-like text.
fn format_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(value) => {
            if *value {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        serde_json::Value::String(value) => {
            serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
        }
        _ => value.to_string(),
    }
}

/// Convert an IR literal into a Python value.
///
/// Example IR:
/// - Literal(int_value=3) -> 3
pub(crate) fn literal_value(lit: &ir::Literal) -> serde_json::Value {
    match lit.value.as_ref() {
        Some(ir::literal::Value::IntValue(value)) => serde_json::Value::Number((*value).into()),
        Some(ir::literal::Value::FloatValue(value)) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(ir::literal::Value::StringValue(value)) => serde_json::Value::String(value.clone()),
        Some(ir::literal::Value::BoolValue(value)) => serde_json::Value::Bool(*value),
        Some(ir::literal::Value::IsNone(_)) => serde_json::Value::Null,
        None => serde_json::Value::Null,
    }
}

/// Try to fold a literal binary operation to a concrete value.
///
/// Example:
/// - (1, 2, BINARY_OP_ADD) -> 3
fn fold_literal_binary(
    op: i32,
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> Option<serde_json::Value> {
    match ir::BinaryOperator::try_from(op).ok() {
        Some(ir::BinaryOperator::BinaryOpAdd) => {
            if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
                return Some(serde_json::Value::Number((left + right).into()));
            }
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left + right).map(serde_json::Value::Number);
            }
            if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
                return Some(serde_json::Value::String(format!("{left}{right}")));
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpSub) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left - right).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpMul) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left * right).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpDiv) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left / right).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpFloorDiv) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                if right == 0.0 {
                    return None;
                }
                let value = (left / right).floor();
                return serde_json::Number::from_f64(value).map(serde_json::Value::Number);
            }
            None
        }
        Some(ir::BinaryOperator::BinaryOpMod) => {
            if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
                return serde_json::Number::from_f64(left % right).map(serde_json::Value::Number);
            }
            None
        }
        _ => None,
    }
}

/// Try to fold a literal unary operation to a concrete value.
///
/// Example:
/// - (UNARY_OP_NEG, 4) -> -4
fn fold_literal_unary(op: i32, operand: &serde_json::Value) -> Option<serde_json::Value> {
    match ir::UnaryOperator::try_from(op).ok() {
        Some(ir::UnaryOperator::UnaryOpNeg) => operand
            .as_f64()
            .and_then(|value| serde_json::Number::from_f64(-value).map(serde_json::Value::Number)),
        Some(ir::UnaryOperator::UnaryOpNot) => Some(serde_json::Value::Bool(!is_truthy(operand))),
        _ => None,
    }
}

impl fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            NodeStatus::Queued => "queued",
            NodeStatus::Running => "running",
            NodeStatus::Completed => "completed",
            NodeStatus::Failed => "failed",
        };
        write!(f, "{value}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::ast as ir;
    use serde_json::Value;

    fn action_plus_two_expr() -> ir::Expr {
        ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: "action_result".to_string(),
                    })),
                    span: None,
                })),
                op: ir::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(2)),
                    })),
                    span: None,
                })),
            }))),
            span: None,
        }
    }

    #[test]
    fn test_runner_state_unrolls_loop_assignments() {
        let mut state = RunnerState::new(None, None, None, true);

        state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                None,
                None,
                Some(0),
            )
            .expect("queue action");
        let first_list = ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: vec![action_plus_two_expr()],
            })),
            span: None,
        };
        state
            .record_assignment(vec!["results".to_string()], &first_list, None, None)
            .expect("record assignment");

        state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                None,
                None,
                Some(1),
            )
            .expect("queue action");
        let second_list = ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: vec![action_plus_two_expr()],
            })),
            span: None,
        };
        let concat_expr = ir::Expr {
            kind: Some(ir::expr::Kind::BinaryOp(Box::new(ir::BinaryOp {
                left: Some(Box::new(ir::Expr {
                    kind: Some(ir::expr::Kind::Variable(ir::Variable {
                        name: "results".to_string(),
                    })),
                    span: None,
                })),
                op: ir::BinaryOperator::BinaryOpAdd as i32,
                right: Some(Box::new(second_list)),
            }))),
            span: None,
        };
        state
            .record_assignment(vec!["results".to_string()], &concat_expr, None, None)
            .expect("record assignment");

        let mut results: Option<ValueExpr> = None;
        for node_id in state.timeline.iter().rev() {
            let node = state.nodes.get(node_id).unwrap();
            if let Some(value) = node.assignments.get("results") {
                results = Some(value.clone());
                break;
            }
        }

        let results = results.expect("results assignment");
        let binary = match results {
            ValueExpr::BinaryOp(value) => value,
            other => panic!("expected BinaryOpValue, got {other:?}"),
        };

        match binary.left.as_ref() {
            ValueExpr::Variable(value) => assert_eq!(value.name, "results"),
            other => panic!("expected VariableValue, got {other:?}"),
        }

        let right_list = match binary.right.as_ref() {
            ValueExpr::List(value) => value,
            other => panic!("expected ListValue, got {other:?}"),
        };
        assert_eq!(right_list.elements.len(), 1);

        let item_bin = match &right_list.elements[0] {
            ValueExpr::BinaryOp(value) => value,
            other => panic!("expected BinaryOpValue, got {other:?}"),
        };

        match item_bin.left.as_ref() {
            ValueExpr::Variable(value) => assert_eq!(value.name, "action_result"),
            other => panic!("expected VariableValue, got {other:?}"),
        }

        match item_bin.right.as_ref() {
            ValueExpr::Literal(value) => assert_eq!(value.value, Value::Number(2.into())),
            other => panic!("expected LiteralValue, got {other:?}"),
        }
    }

    #[test]
    fn test_runner_state_single_target_assignments_stay_symbolic() {
        let mut state = RunnerState::new(None, None, None, true);

        let initial = ValueExpr::Dict(DictValue {
            entries: vec![DictEntryValue {
                key: ValueExpr::Literal(LiteralValue {
                    value: Value::String("result".to_string()),
                }),
                value: ValueExpr::Literal(LiteralValue {
                    value: Value::Number(1.into()),
                }),
            }],
        });
        state
            .record_assignment_value(vec!["result".to_string()], initial, None, None)
            .expect("record initial assignment");

        let wrapped = ValueExpr::Dict(DictValue {
            entries: vec![DictEntryValue {
                key: ValueExpr::Literal(LiteralValue {
                    value: Value::String("result".to_string()),
                }),
                value: ValueExpr::Variable(VariableValue {
                    name: "result".to_string(),
                }),
            }],
        });
        state
            .record_assignment_value(vec!["result".to_string()], wrapped, None, None)
            .expect("record wrapped assignment");

        let mut latest: Option<ValueExpr> = None;
        for node_id in state.timeline.iter().rev() {
            let node = state.nodes.get(node_id).expect("node");
            if let Some(value) = node.assignments.get("result") {
                latest = Some(value.clone());
                break;
            }
        }
        let latest = latest.expect("latest assignment");
        let dict = match latest {
            ValueExpr::Dict(value) => value,
            other => panic!("expected DictValue, got {other:?}"),
        };
        assert_eq!(dict.entries.len(), 1);
        match &dict.entries[0].value {
            ValueExpr::Variable(value) => assert_eq!(value.name, "result"),
            other => panic!("expected VariableValue, got {other:?}"),
        }
    }

    #[test]
    fn test_materialize_value_keeps_self_referential_variable_symbolic() {
        let mut state = RunnerState::new(None, None, None, true);
        state
            .record_assignment_value(
                vec!["count".to_string()],
                ValueExpr::Literal(LiteralValue {
                    value: Value::Number(0.into()),
                }),
                None,
                None,
            )
            .expect("record initial count");
        state
            .record_assignment_value(
                vec!["count".to_string()],
                ValueExpr::BinaryOp(BinaryOpValue {
                    left: Box::new(ValueExpr::Variable(VariableValue {
                        name: "count".to_string(),
                    })),
                    op: ir::BinaryOperator::BinaryOpAdd as i32,
                    right: Box::new(ValueExpr::Literal(LiteralValue {
                        value: Value::Number(1.into()),
                    })),
                }),
                None,
                None,
            )
            .expect("record count update");

        let materialized = state.materialize_value(ValueExpr::Variable(VariableValue {
            name: "count".to_string(),
        }));
        match materialized {
            ValueExpr::Variable(value) => assert_eq!(value.name, "count"),
            other => panic!("expected VariableValue, got {other:?}"),
        }
    }

    #[test]
    fn test_runner_state_graph_dirty_for_action_updates() {
        let mut state = RunnerState::new(None, None, None, true);
        assert!(!state.consume_graph_dirty_for_durable_execution());

        let action_result = state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                None,
                None,
                None,
            )
            .expect("queue action");
        assert!(state.consume_graph_dirty_for_durable_execution());
        assert!(!state.consume_graph_dirty_for_durable_execution());

        state
            .increment_action_attempt(action_result.node_id)
            .expect("increment action attempt");
        assert!(state.consume_graph_dirty_for_durable_execution());
    }

    #[test]
    fn test_runner_state_graph_dirty_not_set_for_assignments() {
        let mut state = RunnerState::new(None, None, None, true);
        let value_expr = ValueExpr::Literal(LiteralValue {
            value: Value::Number(1.into()),
        });
        state
            .record_assignment_value(vec!["value".to_string()], value_expr, None, None)
            .expect("record assignment");

        assert!(!state.consume_graph_dirty_for_durable_execution());
    }

    #[test]
    fn test_runner_state_records_action_start_stop_timestamps() {
        let mut state = RunnerState::new(None, None, None, true);
        let action_result = state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                None,
                None,
                None,
            )
            .expect("queue action");

        // Clear queue-time dirty bit so lifecycle transitions are isolated.
        assert!(state.consume_graph_dirty_for_durable_execution());

        state
            .mark_running(action_result.node_id)
            .expect("mark running");
        let started_at = state
            .nodes
            .get(&action_result.node_id)
            .and_then(|node| node.started_at);
        assert!(
            started_at.is_some(),
            "running action should record started_at"
        );
        assert!(
            state
                .nodes
                .get(&action_result.node_id)
                .and_then(|node| node.completed_at)
                .is_none(),
            "running action should clear completed_at"
        );
        assert!(
            !state.ready_queue.contains(&action_result.node_id),
            "running action should be removed from ready_queue"
        );
        assert!(state.consume_graph_dirty_for_durable_execution());

        state
            .mark_completed(action_result.node_id)
            .expect("mark completed");
        let completed_at = state
            .nodes
            .get(&action_result.node_id)
            .and_then(|node| node.completed_at);
        assert!(
            completed_at.is_some(),
            "completed action should record completed_at"
        );
        assert!(
            completed_at >= started_at,
            "completed_at should be at or after started_at"
        );
        assert!(state.consume_graph_dirty_for_durable_execution());
    }
}
