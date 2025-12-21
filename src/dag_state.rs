//! DAG State Helper - Provides query functions for DAG execution.
//!
//! This module provides a `DAGHelper` that wraps a DAG loaded from the database
//! and provides efficient helper functions for:
//! - Finding input/output boundary nodes
//! - Determining data flow targets
//! - Finding inlinable successor chains
//! - Locating aggregator nodes
//! - Checking node types

#![allow(
    clippy::collapsible_if,
    clippy::option_map_unit_fn,
    clippy::manual_strip
)]

use std::collections::{HashMap, HashSet, VecDeque};

use crate::dag::{DAG, DAGEdge, DAGNode, EdgeType};

/// Determines whether a node can be executed inline or requires delegation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Node can be executed inline (assignments, expressions, control flow)
    Inline,
    /// Node requires delegation to external workers (@actions)
    Delegated,
}

/// Helper for querying and navigating a DAG during execution.
///
/// Provides efficient lookups by pre-computing indexes on construction.
pub struct DAGHelper<'a> {
    dag: &'a DAG,
    /// Outgoing edges indexed by source node ID
    outgoing_edges: HashMap<String, Vec<&'a DAGEdge>>,
    /// Incoming edges indexed by target node ID
    incoming_edges: HashMap<String, Vec<&'a DAGEdge>>,
    /// Input nodes indexed by function name
    input_nodes: HashMap<String, String>,
    /// Output nodes indexed by function name
    output_nodes: HashMap<String, String>,
}

impl<'a> DAGHelper<'a> {
    /// Create a new DAG helper from a DAG reference.
    ///
    /// Pre-computes indexes for efficient lookups.
    pub fn new(dag: &'a DAG) -> Self {
        let mut outgoing_edges: HashMap<String, Vec<&DAGEdge>> = HashMap::new();
        let mut incoming_edges: HashMap<String, Vec<&DAGEdge>> = HashMap::new();
        let mut input_nodes = HashMap::new();
        let mut output_nodes = HashMap::new();

        // Index edges
        for edge in &dag.edges {
            outgoing_edges
                .entry(edge.source.clone())
                .or_default()
                .push(edge);
            incoming_edges
                .entry(edge.target.clone())
                .or_default()
                .push(edge);
        }

        // Index boundary nodes
        for (node_id, node) in &dag.nodes {
            if let Some(ref fn_name) = node.function_name {
                if node.is_input {
                    input_nodes.insert(fn_name.clone(), node_id.clone());
                }
                if node.is_output {
                    output_nodes.insert(fn_name.clone(), node_id.clone());
                }
            }
        }

        Self {
            dag,
            outgoing_edges,
            incoming_edges,
            input_nodes,
            output_nodes,
        }
    }

    // =========================================================================
    // Node Lookups
    // =========================================================================

    /// Get a node by ID.
    pub fn get_node(&self, node_id: &str) -> Option<&'a DAGNode> {
        self.dag.nodes.get(node_id)
    }

    /// Find the input boundary node for a function.
    pub fn find_input_node(&self, function_name: &str) -> Option<&'a DAGNode> {
        self.input_nodes
            .get(function_name)
            .and_then(|id| self.dag.nodes.get(id))
    }

    /// Find the output boundary node for a function.
    pub fn find_output_node(&self, function_name: &str) -> Option<&'a DAGNode> {
        self.output_nodes
            .get(function_name)
            .and_then(|id| self.dag.nodes.get(id))
    }

    /// Get all nodes for a specific function.
    pub fn get_function_nodes(&self, function_name: &str) -> Vec<&'a DAGNode> {
        self.dag
            .nodes
            .values()
            .filter(|n| n.function_name.as_deref() == Some(function_name))
            .collect()
    }

    /// Get all function names in the DAG.
    pub fn get_function_names(&self) -> Vec<&str> {
        self.input_nodes.keys().map(|s| s.as_str()).collect()
    }

    // =========================================================================
    // Edge Lookups
    // =========================================================================

    /// Get all outgoing edges from a node.
    pub fn get_outgoing_edges(&self, node_id: &str) -> &[&'a DAGEdge] {
        self.outgoing_edges
            .get(node_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get all incoming edges to a node.
    pub fn get_incoming_edges(&self, node_id: &str) -> &[&'a DAGEdge] {
        self.incoming_edges
            .get(node_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get outgoing state machine edges (execution order).
    pub fn get_state_machine_successors(&self, node_id: &str) -> Vec<&'a DAGEdge> {
        self.get_outgoing_edges(node_id)
            .iter()
            .filter(|e| {
                e.edge_type == EdgeType::StateMachine
                    && e.exception_types.is_none()
                    && !e
                        .condition
                        .as_deref()
                        .map(|c| c.starts_with("except:"))
                        .unwrap_or(false)
            })
            .copied()
            .collect()
    }

    /// Get outgoing data flow edges (variable passing).
    pub fn get_data_flow_edges(&self, node_id: &str) -> Vec<&'a DAGEdge> {
        self.get_outgoing_edges(node_id)
            .iter()
            .filter(|e| e.edge_type == EdgeType::DataFlow)
            .copied()
            .collect()
    }

    // =========================================================================
    // Data Flow Queries
    // =========================================================================

    /// Get all nodes that should receive data from the given node.
    ///
    /// Returns tuples of (target_node_id, optional_variable_name).
    /// For data flow edges, the variable name indicates which variable to push.
    /// For state machine edges, all outputs are pushed.
    pub fn get_data_flow_targets(&self, node_id: &str) -> Vec<DataFlowTarget> {
        let mut targets = Vec::new();

        for edge in self.get_outgoing_edges(node_id) {
            match edge.edge_type {
                EdgeType::DataFlow => {
                    targets.push(DataFlowTarget {
                        node_id: edge.target.clone(),
                        variable: edge.variable.clone(),
                        edge_type: EdgeType::DataFlow,
                        condition: edge.condition.clone(),
                    });
                }
                EdgeType::StateMachine => {
                    targets.push(DataFlowTarget {
                        node_id: edge.target.clone(),
                        variable: None,
                        edge_type: EdgeType::StateMachine,
                        condition: edge.condition.clone(),
                    });
                }
            }
        }

        targets
    }

    /// Get all source nodes that provide data to the given node.
    pub fn get_data_flow_sources(&self, node_id: &str) -> Vec<DataFlowSource> {
        let mut sources = Vec::new();

        for edge in self.get_incoming_edges(node_id) {
            sources.push(DataFlowSource {
                node_id: edge.source.clone(),
                variable: edge.variable.clone(),
                edge_type: edge.edge_type,
                condition: edge.condition.clone(),
            });
        }

        sources
    }

    // =========================================================================
    // Inlinable Successor Calculation
    // =========================================================================

    /// Get all nodes that can be executed inline starting from a given node.
    ///
    /// This traverses the DAG following state machine edges and collects all
    /// nodes that can be executed without breaking for an @action. Stops when
    /// encountering:
    /// - Delegated nodes (@action calls)
    /// - Aggregator nodes waiting for multiple inputs
    /// - Nodes with conditional branches that aren't resolved yet
    ///
    /// Returns nodes in topological (execution) order.
    pub fn get_inlinable_successors(&self, starting_node_id: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        // Start with immediate successors
        for edge in self.get_state_machine_successors(starting_node_id) {
            if !visited.contains(&edge.target) {
                queue.push_back(edge.target.clone());
                visited.insert(edge.target.clone());
            }
        }

        while let Some(node_id) = queue.pop_front() {
            let node = match self.get_node(&node_id) {
                Some(n) => n,
                None => continue,
            };

            // Check if this node can be inlined
            let mode = self.get_execution_mode(node);

            if mode == ExecutionMode::Delegated {
                // Stop here - this node needs to be queued for external execution
                // But still include it in the result so caller knows about it
                result.push(node_id);
                continue;
            }

            // Aggregator nodes need special handling - check if they can proceed
            if node.is_aggregator {
                // Aggregators typically wait for multiple inputs, but for inline
                // execution we just note them and let the caller handle readiness
                result.push(node_id);
                continue;
            }

            // This node can be inlined
            result.push(node_id.clone());

            // Continue to successors
            for edge in self.get_state_machine_successors(&node_id) {
                if !visited.contains(&edge.target) {
                    queue.push_back(edge.target.clone());
                    visited.insert(edge.target.clone());
                }
            }
        }

        result
    }

    /// Get the immediate state machine successors that are ready for execution.
    ///
    /// Unlike `get_inlinable_successors`, this only returns direct successors
    /// and respects conditional edge labels.
    ///
    /// The DAG structure is "flat" for conditionals and try/except:
    /// - Conditional: "branch" node with guarded edges to each branch action
    /// - Try/except: try body action with success edge and exception edges to handlers
    pub fn get_ready_successors(
        &self,
        node_id: &str,
        condition_result: Option<bool>,
    ) -> Vec<SuccessorInfo> {
        let mut successors = Vec::new();

        for edge in self.get_state_machine_successors(node_id) {
            // Skip loop-back edges - these are only followed when explicitly
            // re-triggering the loop after body completion
            if edge.is_loop_back {
                continue;
            }

            // Handle exception edges - these are only followed when an exception occurs
            // They're identified by having exception_types set
            if edge.exception_types.is_some() {
                // Exception handlers are only activated when explicitly requested
                // During normal traversal, skip them
                continue;
            }

            // Handle default else edges for conditionals.
            // These are evaluated by the runner based on prior guards.
            if edge.is_else {
                let target_node = match self.get_node(&edge.target) {
                    Some(n) => n,
                    None => continue,
                };
                let mode = self.get_execution_mode(target_node);
                successors.push(SuccessorInfo {
                    node_id: edge.target.clone(),
                    execution_mode: mode,
                    is_aggregator: target_node.is_aggregator,
                    condition: edge.condition.clone(),
                    guard_expr: None,
                    is_else: true,
                });
                continue;
            }

            // Handle guarded edges (conditional branches)
            // These have guard_expr set and should be evaluated by the runner
            // For now, we include them in successors and let the runner evaluate
            if edge.guard_expr.is_some() {
                // Include guarded edges - the runner will evaluate them
                let target_node = match self.get_node(&edge.target) {
                    Some(n) => n,
                    None => continue,
                };
                let mode = self.get_execution_mode(target_node);
                successors.push(SuccessorInfo {
                    node_id: edge.target.clone(),
                    execution_mode: mode,
                    is_aggregator: target_node.is_aggregator,
                    condition: edge.condition.clone(),
                    guard_expr: edge.guard_expr.clone(),
                    is_else: false,
                });
                continue;
            }

            // Handle legacy conditional edges (then/else) and other conditions
            if let Some(ref condition) = edge.condition {
                // Legacy try/except handling for backwards compatibility
                if condition.starts_with("except:") {
                    // Exception handlers are only activated when explicitly requested
                    continue;
                }

                match (condition.as_str(), condition_result) {
                    // "success" is followed after a successful try body
                    ("success", _) => {
                        // Edge should be followed
                    }
                    // "try" is always followed (legacy - default success path)
                    ("try", _) => {
                        // Edge should be followed
                    }
                    ("then", Some(true)) | ("else", Some(false)) | (_, None) => {
                        // Edge should be followed
                    }
                    ("then", Some(false)) | ("else", Some(true)) => {
                        // Edge should not be followed
                        continue;
                    }
                    _ => {
                        // Other conditions (parallel, guarded, etc.) - include for now
                    }
                }
            }

            let target_node = match self.get_node(&edge.target) {
                Some(n) => n,
                None => continue,
            };

            let mode = self.get_execution_mode(target_node);

            successors.push(SuccessorInfo {
                node_id: edge.target.clone(),
                execution_mode: mode,
                is_aggregator: target_node.is_aggregator,
                condition: edge.condition.clone(),
                guard_expr: None,
                is_else: false,
            });
        }

        successors
    }

    // =========================================================================
    // Node Type Queries
    // =========================================================================

    /// Determine whether a node should be executed inline or delegated.
    pub fn get_execution_mode(&self, node: &DAGNode) -> ExecutionMode {
        // Action calls are always delegated
        if node.node_type == "action_call" {
            return ExecutionMode::Delegated;
        }

        // External function calls remain as fn_call nodes; treat them as inline no-ops.
        if node.is_fn_call {
            return ExecutionMode::Inline;
        }

        // For-loop nodes are delegated - they need to go through the runner
        // to properly initialize and manage the loop index
        if node.node_type == "for_loop" {
            return ExecutionMode::Delegated;
        }

        // Everything else is inline
        ExecutionMode::Inline
    }

    /// Check if a node is an action call that requires external execution.
    pub fn is_action_node(&self, node_id: &str) -> bool {
        self.get_node(node_id)
            .map(|n| n.node_type == "action_call")
            .unwrap_or(false)
    }

    /// Check if a node is an aggregator (collects spread/parallel results).
    pub fn is_aggregator(&self, node_id: &str) -> bool {
        self.get_node(node_id)
            .map(|n| n.is_aggregator)
            .unwrap_or(false)
    }

    /// Check if a node is a loop head (target of a back-edge).
    pub fn is_loop_head(&self, node_id: &str) -> bool {
        // With normalized loops, the loop head is the branch node that's the target of a back-edge
        self.dag
            .edges
            .iter()
            .any(|e| e.is_loop_back && e.target == node_id)
    }

    /// Check if a node is a conditional branch decision point.
    pub fn is_conditional(&self, node_id: &str) -> bool {
        self.get_node(node_id)
            .map(|n| n.node_type == "branch")
            .unwrap_or(false)
    }

    // =========================================================================
    // Aggregator Queries
    // =========================================================================

    /// Find the aggregator node connected to a spread/parallel node.
    pub fn find_aggregator_for(&self, node_id: &str) -> Option<&'a DAGNode> {
        for edge in self.get_outgoing_edges(node_id) {
            if edge.edge_type == EdgeType::StateMachine {
                if let Some(target_node) = self.get_node(&edge.target) {
                    if target_node.is_aggregator {
                        return Some(target_node);
                    }
                }
            }
        }
        None
    }

    /// Get the source node that an aggregator collects from.
    pub fn get_aggregator_source(&self, aggregator_id: &str) -> Option<&'a DAGNode> {
        self.get_node(aggregator_id)
            .and_then(|n| n.aggregates_from.as_ref())
            .and_then(|source_id| self.get_node(source_id))
    }

    // =========================================================================
    // Exception Handling Queries
    // =========================================================================

    /// Find the try node that a given node is inside of (if any).
    pub fn find_enclosing_try(&self, node_id: &str) -> Option<&'a DAGNode> {
        // Look for an incoming edge with "try" condition
        for edge in self.get_incoming_edges(node_id) {
            if edge.condition.as_deref() == Some("try") {
                return self.get_node(&edge.source);
            }
        }
        None
    }

    /// Get all except handlers for a try node.
    pub fn get_except_handlers(&self, try_node_id: &str) -> Vec<ExceptHandlerInfo> {
        let mut handlers = Vec::new();

        for edge in self.get_outgoing_edges(try_node_id) {
            if let Some(ref condition) = edge.condition {
                if condition.starts_with("except:") {
                    let exception_types = condition[7..].to_string(); // Remove "except:" prefix
                    handlers.push(ExceptHandlerInfo {
                        node_id: edge.target.clone(),
                        exception_types: if exception_types == "*" {
                            vec![] // Catch-all
                        } else {
                            exception_types.split(", ").map(|s| s.to_string()).collect()
                        },
                        is_catch_all: exception_types == "*",
                    });
                }
            }
        }

        handlers
    }

    /// Find the matching except handler for an exception type.
    pub fn find_except_handler(&self, try_node_id: &str, exception_type: &str) -> Option<String> {
        let handlers = self.get_except_handlers(try_node_id);

        // First, look for a specific match
        for handler in &handlers {
            if !handler.is_catch_all
                && handler
                    .exception_types
                    .contains(&exception_type.to_string())
            {
                return Some(handler.node_id.clone());
            }
        }

        // Fall back to catch-all
        for handler in &handlers {
            if handler.is_catch_all {
                return Some(handler.node_id.clone());
            }
        }

        None
    }

    // =========================================================================
    // Parallel Block Queries
    // =========================================================================

    /// Get all call nodes from a parallel block.
    pub fn get_parallel_calls(&self, parallel_node_id: &str) -> Vec<ParallelCallInfo> {
        let mut calls = Vec::new();

        for edge in self.get_outgoing_edges(parallel_node_id) {
            if let Some(ref condition) = edge.condition {
                if condition.starts_with("parallel:") {
                    if let Ok(index) = condition[9..].parse::<usize>() {
                        let node = self.get_node(&edge.target);
                        calls.push(ParallelCallInfo {
                            node_id: edge.target.clone(),
                            index,
                            is_action: node.map(|n| n.node_type == "action_call").unwrap_or(false),
                        });
                    }
                }
            }
        }

        calls.sort_by_key(|c| c.index);
        calls
    }

    // =========================================================================
    // Topological Sort
    // =========================================================================

    /// Get nodes in topological (execution) order for a function.
    pub fn get_execution_order(&self, function_name: &str) -> Vec<String> {
        let fn_nodes: HashSet<String> = self
            .dag
            .get_nodes_for_function(function_name)
            .keys()
            .cloned()
            .collect();

        if fn_nodes.is_empty() {
            return vec![];
        }

        // Build in-degree map
        let mut in_degree: HashMap<String, usize> =
            fn_nodes.iter().map(|n| (n.clone(), 0)).collect();
        let mut adj: HashMap<String, Vec<String>> =
            fn_nodes.iter().map(|n| (n.clone(), Vec::new())).collect();

        for edge in &self.dag.edges {
            if fn_nodes.contains(&edge.source)
                && fn_nodes.contains(&edge.target)
                && edge.edge_type == EdgeType::StateMachine
            {
                adj.get_mut(&edge.source)
                    .map(|v| v.push(edge.target.clone()));
                in_degree.entry(edge.target.clone()).and_modify(|d| *d += 1);
            }
        }

        // Kahn's algorithm
        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(n, _)| n.clone())
            .collect();
        let mut order = Vec::new();

        while let Some(node) = queue.pop_front() {
            order.push(node.clone());
            if let Some(neighbors) = adj.get(&node) {
                for neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(neighbor.clone());
                        }
                    }
                }
            }
        }

        order
    }
}

impl Default for DAGHelper<'_> {
    fn default() -> Self {
        // This is a bit awkward - we need a static DAG for default
        // In practice, always use DAGHelper::new(dag)
        panic!("DAGHelper requires a DAG reference - use DAGHelper::new(dag)")
    }
}

// ============================================================================
// Supporting Types
// ============================================================================

/// Information about a data flow target.
#[derive(Debug, Clone)]
pub struct DataFlowTarget {
    /// Target node ID
    pub node_id: String,
    /// Variable name to push (None for state machine edges)
    pub variable: Option<String>,
    /// Type of edge
    pub edge_type: EdgeType,
    /// Condition on the edge (for conditional branches)
    pub condition: Option<String>,
}

/// Information about a data flow source.
#[derive(Debug, Clone)]
pub struct DataFlowSource {
    /// Source node ID
    pub node_id: String,
    /// Variable name received (for data flow edges)
    pub variable: Option<String>,
    /// Type of edge
    pub edge_type: EdgeType,
    /// Condition on the edge
    pub condition: Option<String>,
}

/// Information about a successor node.
#[derive(Debug, Clone)]
pub struct SuccessorInfo {
    /// Successor node ID
    pub node_id: String,
    /// How the node should be executed
    pub execution_mode: ExecutionMode,
    /// Whether this is an aggregator node
    pub is_aggregator: bool,
    /// Condition on the edge (if any)
    pub condition: Option<String>,
    /// Guard expression for conditional edges (needs evaluation at runtime)
    pub guard_expr: Option<crate::parser::ast::Expr>,
    /// Whether this successor is the default else branch.
    pub is_else: bool,
}

/// Information about an except handler.
#[derive(Debug, Clone)]
pub struct ExceptHandlerInfo {
    /// Handler node ID
    pub node_id: String,
    /// Exception types this handler catches
    pub exception_types: Vec<String>,
    /// Whether this is a catch-all handler
    pub is_catch_all: bool,
}

/// Information about a parallel call.
#[derive(Debug, Clone)]
pub struct ParallelCallInfo {
    /// Call node ID
    pub node_id: String,
    /// Index in the parallel block
    pub index: usize,
    /// Whether this is an action call (vs function call)
    pub is_action: bool,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{convert_to_dag, parse};

    #[test]
    fn test_find_input_output_nodes() {
        let source = r#"fn test(input: [x], output: [y]):
    y = x + 1
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        let input_node = helper.find_input_node("test");
        assert!(input_node.is_some());
        assert!(input_node.unwrap().is_input);

        let output_node = helper.find_output_node("test");
        assert!(output_node.is_some());
        assert!(output_node.unwrap().is_output);
    }

    #[test]
    fn test_get_data_flow_targets() {
        let source = r#"fn test(input: [x], output: [y]):
    y = x + 1
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        let input_node = helper.find_input_node("test").unwrap();
        let targets = helper.get_data_flow_targets(&input_node.id);

        // Should have at least one target (the assignment node)
        assert!(!targets.is_empty());
    }

    #[test]
    fn test_get_execution_mode() {
        let source = r#"fn test(input: [x], output: [y]):
    y = @fetch(id=x)
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        // Find the action call node
        let action_nodes: Vec<_> = dag
            .nodes
            .values()
            .filter(|n| n.node_type == "action_call")
            .collect();
        assert!(!action_nodes.is_empty());

        let mode = helper.get_execution_mode(action_nodes[0]);
        assert_eq!(mode, ExecutionMode::Delegated);
    }

    #[test]
    fn test_get_inlinable_successors() {
        let source = r#"fn test(input: [x], output: [z]):
    y = x + 1
    z = y + 2
    return z"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        let input_node = helper.find_input_node("test").unwrap();
        let inlinable = helper.get_inlinable_successors(&input_node.id);

        // Should include assignment nodes
        assert!(!inlinable.is_empty());
    }

    #[test]
    fn test_is_action_node() {
        let source = r#"fn test(input: [x], output: [y]):
    y = @fetch(id=x)
    return y"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        let action_node_id = dag
            .nodes
            .iter()
            .find(|(_, n)| n.node_type == "action_call")
            .map(|(id, _)| id.clone());

        assert!(action_node_id.is_some());
        assert!(helper.is_action_node(&action_node_id.unwrap()));
    }

    #[test]
    fn test_find_aggregator() {
        let source = r#"fn test(input: [items], output: [results]):
    results = spread items:item -> @fetch(id=item)
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        // Find the spread action node
        let spread_node = dag
            .nodes
            .iter()
            .find(|(_, n)| n.node_type == "action_call")
            .map(|(id, _)| id.clone());

        assert!(spread_node.is_some());

        let aggregator = helper.find_aggregator_for(&spread_node.unwrap());
        assert!(aggregator.is_some());
        assert!(aggregator.unwrap().is_aggregator);
    }

    #[test]
    fn test_get_function_names() {
        // With function expansion, only the entry function is in the final DAG.
        let source = r#"fn main(input: [a, b], output: [result]):
    result = a + b
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        let names = helper.get_function_names();
        assert!(names.contains(&"main"), "Should have entry function 'main'");
    }

    #[test]
    fn test_get_ready_successors_conditional() {
        let source = r#"fn test(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        // Find the branch node (replaces old "if" container node)
        let branch_node_id = dag
            .nodes
            .iter()
            .find(|(_, n)| n.node_type == "branch")
            .map(|(id, _)| id.clone());

        assert!(branch_node_id.is_some());

        // In the new flat DAG structure, the branch node has guarded edges
        // to each branch's first action. All guarded edges are returned and
        // the runner evaluates them.
        let successors = helper.get_ready_successors(&branch_node_id.unwrap(), None);

        // Should have 2 successors: one guarded edge and one default else edge
        assert_eq!(successors.len(), 2, "Branch should have 2 successors");

        let guarded_count = successors.iter().filter(|s| s.guard_expr.is_some()).count();
        let else_count = successors.iter().filter(|s| s.is_else).count();
        assert_eq!(guarded_count, 1, "Expected one guarded successor");
        assert_eq!(else_count, 1, "Expected one else successor");
    }

    #[test]
    fn test_get_execution_order() {
        let source = r#"fn test(input: [x], output: [z]):
    y = x + 1
    z = y + 2
    return z"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        let order = helper.get_execution_order("test");

        // Should have nodes in execution order
        assert!(!order.is_empty());

        // Input should come before output
        let input_pos = order
            .iter()
            .position(|id| dag.nodes.get(id).map(|n| n.is_input).unwrap_or(false));
        let output_pos = order
            .iter()
            .position(|id| dag.nodes.get(id).map(|n| n.is_output).unwrap_or(false));

        assert!(input_pos.is_some());
        assert!(output_pos.is_some());
        assert!(input_pos.unwrap() < output_pos.unwrap());
    }

    #[test]
    fn test_get_parallel_calls() {
        let source = r#"fn test(input: [ids], output: [results]):
    results = parallel:
        @fetch_a(id=1)
        @fetch_b(id=2)
        @fetch_c(id=3)
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        // Find the parallel node
        let parallel_node_id = dag
            .nodes
            .iter()
            .find(|(_, n)| n.node_type == "parallel")
            .map(|(id, _)| id.clone());

        assert!(parallel_node_id.is_some());

        let calls = helper.get_parallel_calls(&parallel_node_id.unwrap());
        assert_eq!(calls.len(), 3);

        // Should be sorted by index
        assert_eq!(calls[0].index, 0);
        assert_eq!(calls[1].index, 1);
        assert_eq!(calls[2].index, 2);
    }

    #[test]
    fn test_get_except_handlers() {
        let source = r#"fn test(input: [x], output: [result]):
    try:
        result = @risky_action(x=x)
    except NetworkError:
        result = @fallback(x=x)
    except:
        result = @error_handler(x=x)
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);

        // In the flat DAG structure, there's no "try" container node.
        // Instead, the try body action (@risky_action) has exception edges
        // to the handler actions. Find the risky_action node.
        let try_body_node_id = dag
            .nodes
            .iter()
            .find(|(_, n)| {
                n.node_type == "action_call" && n.action_name.as_deref() == Some("risky_action")
            })
            .map(|(id, _)| id.clone());

        assert!(
            try_body_node_id.is_some(),
            "Should find try body action node"
        );

        // Look for exception edges from the try body to handlers
        let exception_edges: Vec<_> = dag
            .edges
            .iter()
            .filter(|e| {
                e.source == try_body_node_id.clone().unwrap() && e.exception_types.is_some()
            })
            .collect();

        assert_eq!(
            exception_edges.len(),
            2,
            "Should have 2 exception handler edges"
        );

        // Should have specific handler and catch-all
        let specific = exception_edges.iter().find(|e| {
            e.exception_types
                .as_ref()
                .map(|t| !t.is_empty())
                .unwrap_or(false)
        });
        let catch_all = exception_edges.iter().find(|e| {
            e.exception_types
                .as_ref()
                .map(|t| t.is_empty())
                .unwrap_or(false)
        });

        assert!(specific.is_some(), "Should have specific exception handler");
        assert!(catch_all.is_some(), "Should have catch-all handler");
        assert!(
            specific
                .unwrap()
                .exception_types
                .as_ref()
                .map(|t| t.contains(&"NetworkError".to_string()))
                .unwrap_or(false)
        );
    }

    #[test]
    fn test_is_conditional() {
        let source = r#"fn test(input: [x], output: [result]):
    if x > 0:
        result = "positive"
    else:
        result = "negative"
    return result"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        // Find the branch node (replaces old "if" container node)
        let branch_node_id = dag
            .nodes
            .iter()
            .find(|(_, n)| n.node_type == "branch")
            .map(|(id, _)| id.clone());

        assert!(branch_node_id.is_some());
        assert!(helper.is_conditional(&branch_node_id.unwrap()));
    }

    #[test]
    fn test_loop_structure() {
        // In normalized loop structure, we detect loop heads by finding
        // nodes that are targets of back-edges
        let source = r#"fn test(input: [items], output: [results]):
    results = []
    for item in items:
        x = item + 1
    return results"#;
        let program = parse(source).unwrap();
        let dag = convert_to_dag(&program);
        let helper = DAGHelper::new(&dag);

        // Find back-edges
        let back_edges: Vec<_> = dag.edges.iter().filter(|e| e.is_loop_back).collect();
        assert!(!back_edges.is_empty(), "Should have back-edges for loop");

        // The target of a back-edge is the loop condition node
        let loop_cond_id = &back_edges[0].target;
        let loop_cond = dag
            .nodes
            .get(loop_cond_id)
            .expect("Should find loop condition node");
        assert_eq!(
            loop_cond.node_type, "branch",
            "Loop condition should be a branch node"
        );

        // The loop condition should have guarded successors
        let successors = helper.get_state_machine_successors(loop_cond_id);
        assert!(
            successors.len() >= 2,
            "Loop condition should have at least 2 successors (continue and break)"
        );

        // At least one successor should have a guard (continue path)
        let guarded_successors: Vec<_> = successors
            .iter()
            .filter(|e| e.guard_expr.is_some())
            .collect();
        assert!(
            !guarded_successors.is_empty(),
            "Should have guarded successors for loop condition"
        );
    }
}
