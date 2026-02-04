//! DAG node definitions.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::messages::ast as ir;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InputNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub io_vars: Vec<String>,
}

impl InputNode {
    pub fn new(id: impl Into<String>, io_vars: Vec<String>, function_name: Option<String>) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            io_vars,
        }
    }

    pub fn label(&self) -> String {
        if self.io_vars.is_empty() {
            "input: []".to_string()
        } else {
            format!("input: [{}]", self.io_vars.join(", "))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OutputNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub io_vars: Vec<String>,
}

impl OutputNode {
    pub fn new(id: impl Into<String>, io_vars: Vec<String>, function_name: Option<String>) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            io_vars,
        }
    }

    pub fn label(&self) -> String {
        format!("output: [{}]", self.io_vars.join(", "))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssignmentNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub targets: Vec<String>,
    pub target: Option<String>,
    pub assign_expr: Option<ir::Expr>,
    pub label_hint: Option<String>,
}

impl AssignmentNode {
    pub fn new(
        id: impl Into<String>,
        targets: Vec<String>,
        target: Option<String>,
        assign_expr: Option<ir::Expr>,
        label_hint: Option<String>,
        function_name: Option<String>,
    ) -> Self {
        let fallback = targets.first().cloned();
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            targets,
            target: target.or(fallback),
            assign_expr,
            label_hint,
        }
    }

    pub fn label(&self) -> String {
        if let Some(label) = &self.label_hint {
            return label.clone();
        }
        if self.targets.len() > 1 {
            return format!("{} = ...", self.targets.join(", "));
        }
        let target = self
            .targets
            .first()
            .cloned()
            .or_else(|| self.target.clone())
            .unwrap_or_else(|| "_".to_string());
        format!("{target} = ...")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionCallNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub action_name: String,
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, String>,
    pub kwarg_exprs: HashMap<String, ir::Expr>,
    pub policies: Vec<ir::PolicyBracket>,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
    pub parallel_index: Option<i32>,
    pub aggregates_to: Option<String>,
    pub spread_loop_var: Option<String>,
    pub spread_collection_expr: Option<ir::Expr>,
}

#[derive(Default, Clone)]
pub struct ActionCallParams {
    pub module_name: Option<String>,
    pub kwargs: HashMap<String, String>,
    pub kwarg_exprs: HashMap<String, ir::Expr>,
    pub policies: Vec<ir::PolicyBracket>,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
    pub parallel_index: Option<i32>,
    pub aggregates_to: Option<String>,
    pub spread_loop_var: Option<String>,
    pub spread_collection_expr: Option<ir::Expr>,
    pub function_name: Option<String>,
}

impl ActionCallNode {
    pub fn new(
        id: impl Into<String>,
        action_name: impl Into<String>,
        params: ActionCallParams,
    ) -> Self {
        let ActionCallParams {
            module_name,
            kwargs,
            kwarg_exprs,
            policies,
            targets,
            target,
            parallel_index,
            aggregates_to,
            spread_loop_var,
            spread_collection_expr,
            function_name,
        } = params;
        let fallback = targets.as_ref().and_then(|items| items.first().cloned());
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            action_name: action_name.into(),
            module_name,
            kwargs,
            kwarg_exprs,
            policies,
            targets,
            target: target.or(fallback),
            parallel_index,
            aggregates_to,
            spread_loop_var,
            spread_collection_expr,
        }
    }

    pub fn label(&self) -> String {
        let mut base = format!("@{}()", self.action_name);
        if self.spread_loop_var.is_some() {
            return format!(
                "{base} [spread over {}]",
                self.spread_loop_var.clone().unwrap()
            );
        }
        if let Some(idx) = self.parallel_index {
            base = format!("{base} [{idx}]");
        }
        if let Some(targets) = &self.targets {
            if targets.len() == 1 {
                return format!("{base} -> {}", targets[0]);
            }
            if !targets.is_empty() {
                return format!("{base} -> ({})", targets.join(", "));
            }
        }
        if let Some(target) = &self.target {
            return format!("{base} -> {target}");
        }
        base
    }

    pub fn is_spread(&self) -> bool {
        self.spread_loop_var.is_some() || self.spread_collection_expr.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FnCallNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub called_function: String,
    pub kwargs: HashMap<String, String>,
    pub kwarg_exprs: HashMap<String, ir::Expr>,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
    pub assign_expr: Option<ir::Expr>,
    pub parallel_index: Option<i32>,
    pub aggregates_to: Option<String>,
}

#[derive(Default, Clone)]
pub struct FnCallParams {
    pub kwargs: HashMap<String, String>,
    pub kwarg_exprs: HashMap<String, ir::Expr>,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
    pub assign_expr: Option<ir::Expr>,
    pub parallel_index: Option<i32>,
    pub aggregates_to: Option<String>,
    pub function_name: Option<String>,
}

impl FnCallNode {
    pub fn new(
        id: impl Into<String>,
        called_function: impl Into<String>,
        params: FnCallParams,
    ) -> Self {
        let FnCallParams {
            kwargs,
            kwarg_exprs,
            targets,
            target,
            assign_expr,
            parallel_index,
            aggregates_to,
            function_name,
        } = params;
        let fallback = targets.as_ref().and_then(|items| items.first().cloned());
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            called_function: called_function.into(),
            kwargs,
            kwarg_exprs,
            targets,
            target: target.or(fallback),
            assign_expr,
            parallel_index,
            aggregates_to,
        }
    }

    pub fn label(&self) -> String {
        let mut base = format!("{}()", self.called_function);
        if let Some(idx) = self.parallel_index {
            base = format!("{base} [{idx}]");
        }
        if let Some(targets) = &self.targets {
            if targets.len() == 1 {
                return format!("{base} -> {}", targets[0]);
            }
            if !targets.is_empty() {
                return format!("{base} -> ({})", targets.join(", "));
            }
        }
        if let Some(target) = &self.target {
            return format!("{base} -> {target}");
        }
        base
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParallelNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
}

impl ParallelNode {
    pub fn new(id: impl Into<String>, function_name: Option<String>) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
        }
    }

    pub fn label(&self) -> String {
        "parallel".to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregatorNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub aggregates_from: String,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
    pub aggregator_kind: String,
}

impl AggregatorNode {
    pub fn new(
        id: impl Into<String>,
        aggregates_from: impl Into<String>,
        targets: Option<Vec<String>>,
        target: Option<String>,
        aggregator_kind: impl Into<String>,
        function_name: Option<String>,
    ) -> Self {
        let fallback = targets.as_ref().and_then(|items| items.first().cloned());
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            aggregates_from: aggregates_from.into(),
            targets,
            target: target.or(fallback),
            aggregator_kind: aggregator_kind.into(),
        }
    }

    pub fn label(&self) -> String {
        let prefix = if self.aggregator_kind == "parallel" {
            "parallel_aggregate"
        } else {
            "aggregate"
        };
        if let Some(targets) = &self.targets {
            if targets.len() == 1 {
                return format!("{prefix} -> {}", targets[0]);
            }
            if !targets.is_empty() {
                return format!("{prefix} -> ({})", targets.join(", "));
            }
        }
        if let Some(target) = &self.target {
            return format!("{prefix} -> {target}");
        }
        prefix.to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BranchNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub description: String,
}

impl BranchNode {
    pub fn new(
        id: impl Into<String>,
        description: impl Into<String>,
        function_name: Option<String>,
    ) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            description: description.into(),
        }
    }

    pub fn label(&self) -> String {
        self.description.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JoinNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub description: String,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
}

impl JoinNode {
    pub fn new(
        id: impl Into<String>,
        description: impl Into<String>,
        targets: Option<Vec<String>>,
        target: Option<String>,
        function_name: Option<String>,
    ) -> Self {
        let fallback = targets.as_ref().and_then(|items| items.first().cloned());
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            description: description.into(),
            targets,
            target: target.or(fallback),
        }
    }

    pub fn label(&self) -> String {
        self.description.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReturnNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub assign_expr: Option<ir::Expr>,
    pub targets: Option<Vec<String>>,
    pub target: Option<String>,
}

impl ReturnNode {
    pub fn new(
        id: impl Into<String>,
        assign_expr: Option<ir::Expr>,
        targets: Option<Vec<String>>,
        target: Option<String>,
        function_name: Option<String>,
    ) -> Self {
        let fallback = targets.as_ref().and_then(|items| items.first().cloned());
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            assign_expr,
            targets,
            target: target.or(fallback),
        }
    }

    pub fn label(&self) -> String {
        "return".to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BreakNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
}

impl BreakNode {
    pub fn new(id: impl Into<String>, function_name: Option<String>) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
        }
    }

    pub fn label(&self) -> String {
        "break".to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContinueNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
}

impl ContinueNode {
    pub fn new(id: impl Into<String>, function_name: Option<String>) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
        }
    }

    pub fn label(&self) -> String {
        "continue".to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExpressionNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
}

impl ExpressionNode {
    pub fn new(id: impl Into<String>, function_name: Option<String>) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
        }
    }

    pub fn label(&self) -> String {
        "expr".to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node_type", content = "data")]
pub enum DAGNode {
    Input(InputNode),
    Output(OutputNode),
    Assignment(AssignmentNode),
    ActionCall(ActionCallNode),
    FnCall(FnCallNode),
    Parallel(ParallelNode),
    Aggregator(AggregatorNode),
    Branch(BranchNode),
    Join(JoinNode),
    Return(ReturnNode),
    Break(BreakNode),
    Continue(ContinueNode),
    Expression(ExpressionNode),
}

impl DAGNode {
    pub fn id(&self) -> &str {
        match self {
            DAGNode::Input(node) => &node.id,
            DAGNode::Output(node) => &node.id,
            DAGNode::Assignment(node) => &node.id,
            DAGNode::ActionCall(node) => &node.id,
            DAGNode::FnCall(node) => &node.id,
            DAGNode::Parallel(node) => &node.id,
            DAGNode::Aggregator(node) => &node.id,
            DAGNode::Branch(node) => &node.id,
            DAGNode::Join(node) => &node.id,
            DAGNode::Return(node) => &node.id,
            DAGNode::Break(node) => &node.id,
            DAGNode::Continue(node) => &node.id,
            DAGNode::Expression(node) => &node.id,
        }
    }

    pub fn function_name(&self) -> Option<&str> {
        match self {
            DAGNode::Input(node) => node.function_name.as_deref(),
            DAGNode::Output(node) => node.function_name.as_deref(),
            DAGNode::Assignment(node) => node.function_name.as_deref(),
            DAGNode::ActionCall(node) => node.function_name.as_deref(),
            DAGNode::FnCall(node) => node.function_name.as_deref(),
            DAGNode::Parallel(node) => node.function_name.as_deref(),
            DAGNode::Aggregator(node) => node.function_name.as_deref(),
            DAGNode::Branch(node) => node.function_name.as_deref(),
            DAGNode::Join(node) => node.function_name.as_deref(),
            DAGNode::Return(node) => node.function_name.as_deref(),
            DAGNode::Break(node) => node.function_name.as_deref(),
            DAGNode::Continue(node) => node.function_name.as_deref(),
            DAGNode::Expression(node) => node.function_name.as_deref(),
        }
    }

    pub fn node_uuid(&self) -> &Uuid {
        match self {
            DAGNode::Input(node) => &node.node_uuid,
            DAGNode::Output(node) => &node.node_uuid,
            DAGNode::Assignment(node) => &node.node_uuid,
            DAGNode::ActionCall(node) => &node.node_uuid,
            DAGNode::FnCall(node) => &node.node_uuid,
            DAGNode::Parallel(node) => &node.node_uuid,
            DAGNode::Aggregator(node) => &node.node_uuid,
            DAGNode::Branch(node) => &node.node_uuid,
            DAGNode::Join(node) => &node.node_uuid,
            DAGNode::Return(node) => &node.node_uuid,
            DAGNode::Break(node) => &node.node_uuid,
            DAGNode::Continue(node) => &node.node_uuid,
            DAGNode::Expression(node) => &node.node_uuid,
        }
    }

    pub fn node_type(&self) -> &'static str {
        match self {
            DAGNode::Input(_) => "input",
            DAGNode::Output(_) => "output",
            DAGNode::Assignment(_) => "assignment",
            DAGNode::ActionCall(_) => "action_call",
            DAGNode::FnCall(_) => "fn_call",
            DAGNode::Parallel(_) => "parallel",
            DAGNode::Aggregator(_) => "aggregator",
            DAGNode::Branch(_) => "branch",
            DAGNode::Join(_) => "join",
            DAGNode::Return(_) => "return",
            DAGNode::Break(_) => "break",
            DAGNode::Continue(_) => "continue",
            DAGNode::Expression(_) => "expression",
        }
    }

    pub fn label(&self) -> String {
        match self {
            DAGNode::Input(node) => node.label(),
            DAGNode::Output(node) => node.label(),
            DAGNode::Assignment(node) => node.label(),
            DAGNode::ActionCall(node) => node.label(),
            DAGNode::FnCall(node) => node.label(),
            DAGNode::Parallel(node) => node.label(),
            DAGNode::Aggregator(node) => node.label(),
            DAGNode::Branch(node) => node.label(),
            DAGNode::Join(node) => node.label(),
            DAGNode::Return(node) => node.label(),
            DAGNode::Break(node) => node.label(),
            DAGNode::Continue(node) => node.label(),
            DAGNode::Expression(node) => node.label(),
        }
    }

    pub fn is_input(&self) -> bool {
        matches!(self, DAGNode::Input(_))
    }

    pub fn is_output(&self) -> bool {
        matches!(self, DAGNode::Output(_))
    }

    pub fn is_aggregator(&self) -> bool {
        matches!(self, DAGNode::Aggregator(_))
    }

    pub fn is_fn_call(&self) -> bool {
        matches!(self, DAGNode::FnCall(_))
    }

    pub fn is_spread(&self) -> bool {
        matches!(self, DAGNode::ActionCall(node) if node.is_spread())
    }

    pub fn targets(&self) -> Vec<String> {
        match self {
            DAGNode::Assignment(node) => node.targets.clone(),
            DAGNode::ActionCall(node) => node.targets.clone().unwrap_or_default(),
            DAGNode::FnCall(node) => node.targets.clone().unwrap_or_default(),
            DAGNode::Aggregator(node) => node.targets.clone().unwrap_or_default(),
            DAGNode::Join(node) => node.targets.clone().unwrap_or_default(),
            DAGNode::Return(node) => node.targets.clone().unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    pub fn target(&self) -> Option<String> {
        match self {
            DAGNode::Assignment(node) => node.target.clone(),
            DAGNode::ActionCall(node) => node.target.clone(),
            DAGNode::FnCall(node) => node.target.clone(),
            DAGNode::Aggregator(node) => node.target.clone(),
            DAGNode::Join(node) => node.target.clone(),
            DAGNode::Return(node) => node.target.clone(),
            _ => None,
        }
    }
}

impl From<InputNode> for DAGNode {
    fn from(node: InputNode) -> Self {
        DAGNode::Input(node)
    }
}

impl From<OutputNode> for DAGNode {
    fn from(node: OutputNode) -> Self {
        DAGNode::Output(node)
    }
}

impl From<AssignmentNode> for DAGNode {
    fn from(node: AssignmentNode) -> Self {
        DAGNode::Assignment(node)
    }
}

impl From<ActionCallNode> for DAGNode {
    fn from(node: ActionCallNode) -> Self {
        DAGNode::ActionCall(node)
    }
}

impl From<FnCallNode> for DAGNode {
    fn from(node: FnCallNode) -> Self {
        DAGNode::FnCall(node)
    }
}

impl From<ParallelNode> for DAGNode {
    fn from(node: ParallelNode) -> Self {
        DAGNode::Parallel(node)
    }
}

impl From<AggregatorNode> for DAGNode {
    fn from(node: AggregatorNode) -> Self {
        DAGNode::Aggregator(node)
    }
}

impl From<BranchNode> for DAGNode {
    fn from(node: BranchNode) -> Self {
        DAGNode::Branch(node)
    }
}

impl From<JoinNode> for DAGNode {
    fn from(node: JoinNode) -> Self {
        DAGNode::Join(node)
    }
}

impl From<ReturnNode> for DAGNode {
    fn from(node: ReturnNode) -> Self {
        DAGNode::Return(node)
    }
}

impl From<BreakNode> for DAGNode {
    fn from(node: BreakNode) -> Self {
        DAGNode::Break(node)
    }
}

impl From<ContinueNode> for DAGNode {
    fn from(node: ContinueNode) -> Self {
        DAGNode::Continue(node)
    }
}

impl From<ExpressionNode> for DAGNode {
    fn from(node: ExpressionNode) -> Self {
        DAGNode::Expression(node)
    }
}
