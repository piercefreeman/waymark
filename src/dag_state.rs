//! DAG state management for workflow execution.
//!
//! This module provides utilities for tracking DAG execution state,
//! including dependency satisfaction and node readiness.

use crate::dag::{Dag, EdgeKind, Node};
use std::collections::HashSet;

/// Tracks the execution state of a workflow instance.
#[derive(Debug, Clone, Default)]
pub struct InstanceDagState {
    /// Nodes that have been seen/scheduled
    known: HashSet<String>,
    /// Nodes that have completed execution
    completed: HashSet<String>,
}

impl InstanceDagState {
    pub fn new(known: HashSet<String>, completed: HashSet<String>) -> Self {
        Self { known, completed }
    }

    pub fn record_known(&mut self, node_id: impl Into<String>) {
        self.known.insert(node_id.into());
    }

    pub fn record_completion(&mut self, node_id: impl Into<String>) {
        let node = node_id.into();
        self.completed.insert(node.clone());
        self.known.insert(node);
    }

    pub fn known(&self) -> &HashSet<String> {
        &self.known
    }

    pub fn completed(&self) -> &HashSet<String> {
        &self.completed
    }
}

/// State machine for DAG-based workflow execution.
pub struct DagStateMachine<'a> {
    dag: &'a Dag,
}

impl<'a> DagStateMachine<'a> {
    pub fn new(dag: &'a Dag) -> Self {
        Self { dag }
    }

    /// Get nodes that are ready to execute.
    pub fn ready_nodes(&self, state: &InstanceDagState) -> Vec<&Node> {
        self.dag
            .nodes
            .values()
            .filter(|node| {
                // Skip if already known/scheduled
                if state.known.contains(&node.id) {
                    return false;
                }
                // Check if dependencies are satisfied
                deps_satisfied(node, self.dag, &state.completed)
            })
            .collect()
    }

    /// Mark a node as completed and return newly ready nodes.
    pub fn ready_after_completion(
        &self,
        state: &mut InstanceDagState,
        completed_node: Option<&str>,
    ) -> Vec<&Node> {
        if let Some(node_id) = completed_node {
            state.record_completion(node_id);
        }
        self.ready_nodes(state)
    }
}

/// Check if a node's dependencies are satisfied.
///
/// This excludes back edges (handled by loop_head evaluation)
/// and guarded edges (handled by branch evaluation).
pub fn deps_satisfied(node: &Node, dag: &Dag, completed: &HashSet<String>) -> bool {
    // Find all nodes that have edges pointing to this node
    for (source_id, source_node) in &dag.nodes {
        for edge in &source_node.edges {
            if edge.target == node.id {
                // Skip back edges and guarded edges - they're handled specially
                match edge.kind {
                    EdgeKind::Back | EdgeKind::GuardTrue | EdgeKind::GuardFalse | EdgeKind::Exception => {
                        continue;
                    }
                    EdgeKind::Data | EdgeKind::Continue | EdgeKind::Exit => {
                        if !completed.contains(source_id) {
                            return false;
                        }
                    }
                }
            }
        }
    }
    true
}

/// Get downstream nodes from a given node by edge type.
pub fn get_downstream_by_edge_type(node_id: &str, dag: &Dag, edge_type: EdgeKind) -> Vec<String> {
    dag.nodes
        .get(node_id)
        .map(|node| {
            node.edges
                .iter()
                .filter(|e| e.kind == edge_type)
                .map(|e| e.target.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Get the back edge target (loop head) for a node, if any.
pub fn get_back_edge_target(node_id: &str, dag: &Dag) -> Option<String> {
    dag.nodes.get(node_id).and_then(|node| {
        node.edges
            .iter()
            .find(|e| e.kind == EdgeKind::Back)
            .map(|e| e.target.clone())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{LoopHeadMeta, Node};

    fn build_state() -> InstanceDagState {
        InstanceDagState::new(HashSet::new(), HashSet::new())
    }

    #[test]
    fn test_simple_deps() {
        let mut dag = Dag::new(false);

        let mut node_a = Node::action("a", "action_a");
        node_a.add_edge("b", EdgeKind::Data);

        let node_b = Node::action("b", "action_b");

        dag.add_node(node_a);
        dag.add_node(node_b);

        let completed = HashSet::new();

        // Node A has no deps, should be ready
        assert!(deps_satisfied(dag.get_node("a").unwrap(), &dag, &completed));

        // Node B depends on A, should not be ready
        assert!(!deps_satisfied(dag.get_node("b").unwrap(), &dag, &completed));

        // After A completes, B should be ready
        let mut completed = HashSet::new();
        completed.insert("a".to_string());
        assert!(deps_satisfied(dag.get_node("b").unwrap(), &dag, &completed));
    }

    #[test]
    fn test_back_edge_ignored() {
        let mut dag = Dag::new(false);

        let loop_meta = LoopHeadMeta {
            iterator_var: "items".to_string(),
            loop_var: "item".to_string(),
            body_entry: "body".to_string(),
            body_tail: "body".to_string(),
            body_nodes: ["body".to_string()].into_iter().collect(),
        };

        let mut loop_head = Node::loop_head("loop_head", loop_meta);
        loop_head.add_edge("body", EdgeKind::Continue);

        let mut body = Node::action("body", "process");
        body.add_edge("loop_head", EdgeKind::Back);

        dag.add_node(loop_head);
        dag.add_node(body);

        // Loop head should be ready even though body has a back edge to it
        let completed = HashSet::new();
        assert!(deps_satisfied(
            dag.get_node("loop_head").unwrap(),
            &dag,
            &completed
        ));
    }

    #[test]
    fn test_state_machine() {
        let mut dag = Dag::new(false);

        let mut node_a = Node::action("a", "action_a");
        node_a.add_edge("b", EdgeKind::Data);
        dag.add_node(node_a);

        let mut node_b = Node::action("b", "action_b");
        node_b.add_edge("c", EdgeKind::Data);
        dag.add_node(node_b);

        let node_c = Node::action("c", "action_c");
        dag.add_node(node_c);

        let machine = DagStateMachine::new(&dag);
        let mut state = build_state();

        // Initially only A should be ready
        let ready = machine.ready_nodes(&state);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "a");

        // After marking A as known/completed, B should be ready
        let ready = machine.ready_after_completion(&mut state, Some("a"));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "b");

        // After marking B as completed, C should be ready
        let ready = machine.ready_after_completion(&mut state, Some("b"));
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "c");
    }
}
