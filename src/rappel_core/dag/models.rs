//! Core DAG models and shared helpers.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::messages::ast as ir;

use super::nodes::DAGNode;

pub const EXCEPTION_SCOPE_VAR: &str = "__rappel_exception__";

/// Raised when IR -> DAG conversion fails.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct DagConversionError(pub String);

/// Classifies edges as control-flow (state machine) or data-flow.
///
/// We keep the distinction so visualization and scheduling can render them
/// differently (solid vs dashed) and so data dependencies can be computed
/// independently of execution ordering.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    StateMachine,
    DataFlow,
}

/// Directed edge between DAG nodes with execution or data semantics.
///
/// We store rich metadata because both the runtime and the visualizer need to
/// interpret the same graph: control-flow edges carry conditions/guards, while
/// data-flow edges track which variable definition feeds which consumer.
///
/// Visualization examples:
/// - control: action_1 -> join_2 (condition="success")
/// - control: branch_3 -> then_4 (condition="guarded")
/// - data: assign_5 -> action_6 (variable="payload")
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DAGEdge {
    pub source: String,
    pub target: String,
    pub edge_type: EdgeType,
    pub condition: Option<String>,
    pub variable: Option<String>,
    pub guard_expr: Option<ir::Expr>,
    pub is_else: bool,
    pub exception_types: Option<Vec<String>>,
    pub exception_depth: Option<i32>,
    pub is_loop_back: bool,
    pub guard_string: Option<String>,
}

impl DAGEdge {
    pub fn state_machine(source: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            target: target.into(),
            edge_type: EdgeType::StateMachine,
            condition: None,
            variable: None,
            guard_expr: None,
            is_else: false,
            exception_types: None,
            exception_depth: None,
            is_loop_back: false,
            guard_string: None,
        }
    }

    pub fn state_machine_with_condition(
        source: impl Into<String>,
        target: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        let mut edge = Self::state_machine(source, target);
        edge.condition = Some(condition.into());
        edge
    }

    pub fn state_machine_with_guard(
        source: impl Into<String>,
        target: impl Into<String>,
        guard: ir::Expr,
    ) -> Self {
        let mut edge = Self::state_machine(source, target);
        edge.condition = Some("guarded".to_string());
        edge.guard_expr = Some(guard);
        edge
    }

    pub fn state_machine_else(source: impl Into<String>, target: impl Into<String>) -> Self {
        let mut edge = Self::state_machine(source, target);
        edge.condition = Some("else".to_string());
        edge.is_else = true;
        edge
    }

    pub fn state_machine_with_exception(
        source: impl Into<String>,
        target: impl Into<String>,
        exception_types: Vec<String>,
    ) -> Self {
        let normalized = if exception_types.len() == 1 && exception_types[0] == "Exception" {
            Vec::new()
        } else {
            exception_types
        };
        let condition = if normalized.is_empty() {
            "except:*".to_string()
        } else {
            format!("except:{}", normalized.join(","))
        };
        let mut edge = Self::state_machine(source, target);
        edge.condition = Some(condition);
        edge.exception_types = Some(normalized);
        edge
    }

    pub fn state_machine_success(source: impl Into<String>, target: impl Into<String>) -> Self {
        let mut edge = Self::state_machine(source, target);
        edge.condition = Some("success".to_string());
        edge
    }

    pub fn data_flow(
        source: impl Into<String>,
        target: impl Into<String>,
        variable: impl Into<String>,
    ) -> Self {
        let mut edge = Self::state_machine(source, target);
        edge.edge_type = EdgeType::DataFlow;
        edge.variable = Some(variable.into());
        edge
    }

    pub fn with_loop_back(mut self, is_loop_back: bool) -> Self {
        self.is_loop_back = is_loop_back;
        self
    }

    pub fn with_guard(mut self, guard: impl Into<String>) -> Self {
        self.guard_string = Some(guard.into());
        self
    }
}

/// Container for DAG nodes/edges with helper queries.
///
/// The DAG object is the common currency between conversion, scheduling, and
/// visualization. We keep both node metadata and edge metadata so downstream
/// tools can render a faithful control/data graph.
///
/// Visualization example (pseudo):
/// - nodes: input -> action -> output
/// - edges: input -control-> action, action -data(var=x)-> output
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DAG {
    pub nodes: HashMap<String, DAGNode>,
    pub edges: Vec<DAGEdge>,
    pub entry_node: Option<String>,
}

impl DAG {
    pub fn add_node(&mut self, node: DAGNode) {
        if self.entry_node.is_none() {
            self.entry_node = Some(node.id().to_string());
        }
        self.nodes.insert(node.id().to_string(), node);
    }

    pub fn add_edge(&mut self, edge: DAGEdge) {
        self.edges.push(edge);
    }

    pub fn get_incoming_edges(&self, node_id: &str) -> Vec<DAGEdge> {
        self.edges
            .iter()
            .filter(|edge| edge.target == node_id)
            .cloned()
            .collect()
    }

    pub fn get_outgoing_edges(&self, node_id: &str) -> Vec<DAGEdge> {
        self.edges
            .iter()
            .filter(|edge| edge.source == node_id)
            .cloned()
            .collect()
    }

    pub fn get_state_machine_edges(&self) -> Vec<DAGEdge> {
        self.edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::StateMachine)
            .cloned()
            .collect()
    }

    pub fn get_data_flow_edges(&self) -> Vec<DAGEdge> {
        self.edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::DataFlow)
            .cloned()
            .collect()
    }

    pub fn get_functions(&self) -> Vec<String> {
        let mut functions: HashSet<String> = HashSet::new();
        for node in self.nodes.values() {
            if let Some(name) = node.function_name() {
                functions.insert(name.to_string());
            }
        }
        let mut sorted: Vec<String> = functions.into_iter().collect();
        sorted.sort();
        sorted
    }

    pub fn get_nodes_for_function(&self, function_name: &str) -> HashMap<String, DAGNode> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.function_name() == Some(function_name))
            .map(|(node_id, node)| (node_id.clone(), node.clone()))
            .collect()
    }

    pub fn get_edges_for_function(&self, function_name: &str) -> Vec<DAGEdge> {
        let fn_nodes: HashSet<String> = self
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();
        self.edges
            .iter()
            .filter(|edge| fn_nodes.contains(&edge.source) && fn_nodes.contains(&edge.target))
            .cloned()
            .collect()
    }
}

/// Intermediate representation for stitching statement subgraphs.
///
/// Every IR statement can expand into multiple DAG nodes. ConvertedSubgraph
/// captures the "entry" and "exits" so the converter can wire the next
/// statement without knowing the internal structure of the previous one.
///
/// Examples:
/// - Simple assignment: entry=assign_1, exits=[assign_1]
/// - If/else: entry=branch_2, exits=[join_5]
/// - Empty block: is_noop=True (frontier stays unchanged)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConvertedSubgraph {
    pub entry: Option<String>,
    pub exits: Vec<String>,
    pub nodes: Vec<String>,
    pub is_noop: bool,
}

impl ConvertedSubgraph {
    pub fn noop() -> Self {
        Self {
            entry: None,
            exits: Vec::new(),
            nodes: Vec::new(),
            is_noop: true,
        }
    }
}
