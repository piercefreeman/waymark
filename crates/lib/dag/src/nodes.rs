//! DAG node definitions.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use waymark_proto::ast as ir;

/// Function entry node that declares input variables.
///
/// Visualization example: label="input: [base, limit]"
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

/// Function output node that exposes return values.
///
/// Visualization example: label="output: [result]"
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

/// Represents assignment statements in the DAG.
///
/// Visualization example: label="total = ..."
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

/// Invokes an action via @action() with optional parallel/spread context.
///
/// Visualization examples:
/// - label="@double() -> value"
/// - label="@fetch() [spread over item]"
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

/// Invokes a user-defined function inside the DAG.
///
/// Visualization example: label="helper() -> output"
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

/// Parallel fan-out control node.
///
/// Visualization example: label="parallel"
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

/// Collects outputs from parallel or spread branches.
///
/// Visualization examples:
/// - label="aggregate -> result"
/// - label="parallel_aggregate -> outputs"
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

/// Conditional branch dispatch node.
///
/// Visualization example: label="if guard"
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

/// Converges multiple control-flow branches.
///
/// Visualization example: label="join"
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

/// Return node used when expanding nested functions.
///
/// Visualization example: label="return"
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

/// Loop break node.
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

/// Loop continue node.
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

/// Durable sleep node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SleepNode {
    pub id: String,
    pub node_uuid: Uuid,
    pub function_name: Option<String>,
    pub duration_expr: Option<ir::Expr>,
    pub label_hint: Option<String>,
}

impl SleepNode {
    pub fn new(
        id: impl Into<String>,
        duration_expr: Option<ir::Expr>,
        label_hint: Option<String>,
        function_name: Option<String>,
    ) -> Self {
        Self {
            id: id.into(),
            node_uuid: Uuid::new_v4(),
            function_name,
            duration_expr,
            label_hint,
        }
    }

    pub fn label(&self) -> String {
        self.label_hint
            .clone()
            .unwrap_or_else(|| "sleep".to_string())
    }
}

/// Bare expression statement node.
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
