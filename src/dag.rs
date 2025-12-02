//! Internal DAG representation for workflow execution.
//!
//! This module defines the internal DAG structure used by the scheduler.
//! The DAG is produced by converting IR and is NOT exposed via protobuf.
//! Only the IR representation crosses the Python<->Rust boundary.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Kinds of nodes in the DAG
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    /// Regular action executed by workers
    Action,
    /// Inline Python computation (executed by worker but not durable)
    Computed,
    /// Loop control node (scheduler-evaluated, not dispatched)
    LoopHead,
    /// Joins parallel branches back together
    GatherJoin,
    /// Conditional branch point
    Branch,
    /// Try block entry point
    TryHead,
    /// Durable sleep
    Sleep,
    /// Workflow return point
    Return,
}

/// Edge types for DAG traversal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeKind {
    /// Normal data dependency
    Data,
    /// Loop continues (more iterations)
    Continue,
    /// Back edge to loop head
    Back,
    /// Loop exits (iterator exhausted)
    Exit,
    /// Conditional true branch
    GuardTrue,
    /// Conditional false branch
    GuardFalse,
    /// Exception handler edge
    Exception,
}

/// An edge in the DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub target: String,
    pub kind: EdgeKind,
    /// For exception edges: the exception type to match (empty = catch all)
    pub exception_type: Option<String>,
    pub exception_module: Option<String>,
}

/// Metadata for loop head nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopHeadMeta {
    /// Variable name containing the iterator (looked up in eval_context)
    pub iterator_var: String,
    /// Variable name to bind current item to
    pub loop_var: String,
    /// First node in loop body
    pub body_entry: String,
    /// Last node in loop body (source of back edge)
    pub body_tail: String,
    /// All nodes in the loop body (for resetting on each iteration)
    pub body_nodes: HashSet<String>,
}

/// Metadata for branch nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchMeta {
    /// Guard expression to evaluate
    pub guard_expr: String,
    /// Entry node for true branch (if any)
    pub true_entry: Option<String>,
    /// Entry node for false branch (if any)
    pub false_entry: Option<String>,
    /// All nodes in true branch
    pub true_nodes: HashSet<String>,
    /// All nodes in false branch
    pub false_nodes: HashSet<String>,
    /// Node where branches merge (if any)
    pub merge_node: Option<String>,
}

/// Metadata for try/except nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TryExceptMeta {
    /// All nodes in the try block
    pub try_nodes: HashSet<String>,
    /// Handler entry points with their exception types
    pub handlers: Vec<HandlerMeta>,
    /// Node where try/handlers merge
    pub merge_node: Option<String>,
}

/// Metadata for a single exception handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandlerMeta {
    /// Entry node for this handler
    pub entry: String,
    /// All nodes in this handler
    pub nodes: HashSet<String>,
    /// Exception types this handler catches (empty = catch all)
    pub exception_types: Vec<(Option<String>, Option<String>)>, // (module, name)
}

/// Backoff policy for retries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BackoffPolicy {
    Linear { base_delay_ms: u32 },
    Exponential { base_delay_ms: u32, multiplier: f64 },
}

/// A node in the DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub kind: NodeKind,

    // For action/computed nodes
    pub action: Option<String>,
    pub module: Option<String>,
    pub kwargs: HashMap<String, String>,  // Expression strings

    // What this node produces
    pub produces: Vec<String>,

    // Outgoing edges
    pub edges: Vec<Edge>,

    // Execution policy
    pub timeout_seconds: Option<u32>,
    pub max_retries: Option<u32>,
    pub backoff: Option<BackoffPolicy>,

    // Metadata for special node types
    pub loop_meta: Option<LoopHeadMeta>,
    pub branch_meta: Option<BranchMeta>,
    pub try_except_meta: Option<TryExceptMeta>,

    // For body nodes: which loop they belong to (for reset tracking)
    pub loop_id: Option<String>,

    // For sleep nodes: duration expression
    pub sleep_duration_expr: Option<String>,
}

impl Node {
    /// Create a new action node
    pub fn action(id: impl Into<String>, action: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::Action,
            action: Some(action.into()),
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: None,
            branch_meta: None,
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: None,
        }
    }

    /// Create a new computed (python_block) node
    pub fn computed(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::Computed,
            action: Some("python_block".into()),
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: None,
            branch_meta: None,
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: None,
        }
    }

    /// Create a new loop head node
    pub fn loop_head(id: impl Into<String>, meta: LoopHeadMeta) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::LoopHead,
            action: None,
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: Some(meta),
            branch_meta: None,
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: None,
        }
    }

    /// Create a gather join node
    pub fn gather_join(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::GatherJoin,
            action: None,
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: None,
            branch_meta: None,
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: None,
        }
    }

    /// Create a branch node
    pub fn branch(id: impl Into<String>, meta: BranchMeta) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::Branch,
            action: None,
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: None,
            branch_meta: Some(meta),
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: None,
        }
    }

    /// Create a sleep node
    pub fn sleep(id: impl Into<String>, duration_expr: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::Sleep,
            action: None,
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: None,
            branch_meta: None,
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: Some(duration_expr.into()),
        }
    }

    /// Create a return node
    pub fn return_node(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: NodeKind::Return,
            action: None,
            module: None,
            kwargs: HashMap::new(),
            produces: Vec::new(),
            edges: Vec::new(),
            timeout_seconds: None,
            max_retries: None,
            backoff: None,
            loop_meta: None,
            branch_meta: None,
            try_except_meta: None,
            loop_id: None,
            sleep_duration_expr: None,
        }
    }

    /// Add an edge to another node
    pub fn add_edge(&mut self, target: impl Into<String>, kind: EdgeKind) {
        self.edges.push(Edge {
            target: target.into(),
            kind,
            exception_type: None,
            exception_module: None,
        });
    }

    /// Add an exception edge
    pub fn add_exception_edge(
        &mut self,
        target: impl Into<String>,
        exception_type: Option<String>,
        exception_module: Option<String>,
    ) {
        self.edges.push(Edge {
            target: target.into(),
            kind: EdgeKind::Exception,
            exception_type,
            exception_module,
        });
    }

    /// Set module
    pub fn with_module(mut self, module: impl Into<String>) -> Self {
        self.module = Some(module.into());
        self
    }

    /// Add a kwarg
    pub fn with_kwarg(mut self, key: impl Into<String>, expr: impl Into<String>) -> Self {
        self.kwargs.insert(key.into(), expr.into());
        self
    }

    /// Set produces
    pub fn with_produces(mut self, vars: Vec<String>) -> Self {
        self.produces = vars;
        self
    }

    /// Set loop_id
    pub fn in_loop(mut self, loop_id: impl Into<String>) -> Self {
        self.loop_id = Some(loop_id.into());
        self
    }
}

/// The complete DAG for a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dag {
    pub nodes: HashMap<String, Node>,
    pub entry_nodes: Vec<String>,  // Nodes with no dependencies
    pub return_variable: Option<String>,
    pub concurrent: bool,
}

impl Dag {
    pub fn new(concurrent: bool) -> Self {
        Self {
            nodes: HashMap::new(),
            entry_nodes: Vec::new(),
            return_variable: None,
            concurrent,
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn get_node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id)
    }

    pub fn get_node_mut(&mut self, id: &str) -> Option<&mut Node> {
        self.nodes.get_mut(id)
    }

    /// Compute entry nodes (nodes with no incoming edges)
    pub fn compute_entry_nodes(&mut self) {
        let all_targets: HashSet<String> = self.nodes.values()
            .flat_map(|n| n.edges.iter().map(|e| e.target.clone()))
            .collect();

        self.entry_nodes = self.nodes.keys()
            .filter(|id| !all_targets.contains(*id))
            .cloned()
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_dag() {
        let mut dag = Dag::new(false);

        let mut node1 = Node::action("action_0", "fetch");
        node1.produces = vec!["data".into()];
        node1.add_edge("action_1", EdgeKind::Data);

        let node2 = Node::action("action_1", "process")
            .with_kwarg("input", "data");

        dag.add_node(node1);
        dag.add_node(node2);
        dag.compute_entry_nodes();

        assert_eq!(dag.entry_nodes, vec!["action_0"]);
        assert_eq!(dag.nodes.len(), 2);
    }

    #[test]
    fn test_loop_dag() {
        let mut dag = Dag::new(false);

        // Create loop head
        let loop_meta = LoopHeadMeta {
            iterator_var: "items".into(),
            loop_var: "item".into(),
            body_entry: "action_0".into(),
            body_tail: "action_0".into(),
            body_nodes: ["action_0".into()].into_iter().collect(),
        };

        let mut loop_head = Node::loop_head("loop_0_head", loop_meta);
        loop_head.add_edge("action_0", EdgeKind::Continue);

        let mut action = Node::action("action_0", "process")
            .with_kwarg("item", "item")
            .in_loop("loop_0");
        action.add_edge("loop_0_head", EdgeKind::Back);

        dag.add_node(loop_head);
        dag.add_node(action);

        // Verify structure
        assert_eq!(dag.nodes.len(), 2);
        let head = dag.get_node("loop_0_head").unwrap();
        assert_eq!(head.kind, NodeKind::LoopHead);
        assert!(head.loop_meta.is_some());

        let body = dag.get_node("action_0").unwrap();
        assert_eq!(body.loop_id, Some("loop_0".into()));
    }
}
