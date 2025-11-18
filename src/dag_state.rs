use std::collections::HashSet;

use crate::messages::proto::{WorkflowDagDefinition, WorkflowDagNode};

#[derive(Debug, Clone, Default)]
pub struct InstanceDagState {
    known: HashSet<String>,
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

pub struct DagStateMachine<'a> {
    dag: &'a WorkflowDagDefinition,
    concurrent: bool,
}

impl<'a> DagStateMachine<'a> {
    pub fn new(dag: &'a WorkflowDagDefinition, concurrent: bool) -> Self {
        Self { dag, concurrent }
    }

    pub fn ready_nodes(&self, state: &InstanceDagState) -> Vec<WorkflowDagNode> {
        determine_unlocked_nodes(self.dag, state.known(), state.completed(), self.concurrent)
    }

    pub fn ready_after_completion(
        &self,
        state: &mut InstanceDagState,
        completed_node: Option<&str>,
    ) -> Vec<WorkflowDagNode> {
        if let Some(node_id) = completed_node {
            state.record_completion(node_id.to_string());
        }
        self.ready_nodes(state)
    }
}

fn determine_unlocked_nodes(
    dag: &WorkflowDagDefinition,
    known_nodes: &HashSet<String>,
    completed_nodes: &HashSet<String>,
    concurrent: bool,
) -> Vec<WorkflowDagNode> {
    dag.nodes
        .iter()
        .filter(|node| {
            if known_nodes.contains(&node.id) {
                return false;
            }
            required_dependencies(node, concurrent)
                .into_iter()
                .all(|dep| completed_nodes.contains(dep))
        })
        .cloned()
        .collect()
}

fn required_dependencies(node: &WorkflowDagNode, concurrent: bool) -> Vec<&str> {
    let mut deps: Vec<&str> = node.depends_on.iter().map(|s| s.as_str()).collect();
    if !concurrent {
        deps.extend(node.wait_for_sync.iter().map(|s| s.as_str()));
    }
    deps
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_node(id: &str, depends_on: &[&str], wait_for_sync: &[&str]) -> WorkflowDagNode {
        WorkflowDagNode {
            id: id.to_string(),
            action: format!("action_{id}"),
            kwargs: Default::default(),
            depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
            wait_for_sync: wait_for_sync.iter().map(|s| s.to_string()).collect(),
            module: String::new(),
            produces: Vec::new(),
            guard: String::new(),
        }
    }

    fn build_state() -> InstanceDagState {
        InstanceDagState::new(HashSet::new(), HashSet::new())
    }

    #[test]
    fn sequential_workflows_respect_wait_for_sync() {
        let dag = WorkflowDagDefinition {
            concurrent: false,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &[], &["node_0"]),
            ],
        };
        let machine = DagStateMachine::new(&dag, false);
        let mut state = build_state();
        let ready = machine.ready_nodes(&state);
        assert_eq!(
            vec!["node_0"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
        state.record_completion("node_0");
        let ready = machine.ready_nodes(&state);
        assert_eq!(
            vec!["node_1"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn concurrent_workflows_ignore_wait_for_sync() {
        let dag = WorkflowDagDefinition {
            concurrent: true,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &[], &["node_0"]),
            ],
        };
        let machine = DagStateMachine::new(&dag, true);
        let state = build_state();
        let ready = machine.ready_nodes(&state);
        let ids: Vec<&str> = ready.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(vec!["node_0", "node_1"], ids);
    }

    #[test]
    fn dependencies_gate_unlocking() {
        let dag = WorkflowDagDefinition {
            concurrent: true,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &["node_0"], &[]),
            ],
        };
        let machine = DagStateMachine::new(&dag, true);
        let mut state = build_state();
        let ready = machine.ready_nodes(&state);
        assert_eq!(
            vec!["node_0"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
        state.record_completion("node_0");
        let ready = machine.ready_nodes(&state);
        assert_eq!(
            vec!["node_1"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn ready_after_completion_marks_state() {
        let dag = WorkflowDagDefinition {
            concurrent: false,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &["node_0"], &[]),
                build_node("node_2", &["node_1"], &[]),
            ],
        };
        let machine = DagStateMachine::new(&dag, false);
        let mut state = build_state();
        let ready = machine.ready_after_completion(&mut state, None);
        assert_eq!(
            vec!["node_0"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
        let ready = machine.ready_after_completion(&mut state, Some("node_0"));
        assert_eq!(
            vec!["node_1"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn guard_nodes_unlock_like_regular_nodes() {
        let guarded = WorkflowDagNode {
            id: "node_guard".to_string(),
            action: "action_guard".to_string(),
            kwargs: Default::default(),
            depends_on: vec!["node_0".to_string()],
            wait_for_sync: Vec::new(),
            module: String::new(),
            produces: Vec::new(),
            guard: "user_flag".to_string(),
        };
        let dag = WorkflowDagDefinition {
            concurrent: false,
            nodes: vec![build_node("node_0", &[], &[]), guarded],
        };
        let machine = DagStateMachine::new(&dag, false);
        let mut state = build_state();
        let ready = machine.ready_nodes(&state);
        assert_eq!(
            vec!["node_0"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
        state.record_completion("node_0");
        let ready = machine.ready_nodes(&state);
        assert_eq!(
            vec!["node_guard"],
            ready.iter().map(|n| n.id.as_str()).collect::<Vec<_>>()
        );
    }
}
