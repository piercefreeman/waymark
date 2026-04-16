use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use waymark_dag::EdgeType;
use waymark_ids::ExecutionId;

use crate::{RunnerStateError, value::*};

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
    pub node_id: ExecutionId,
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionEdge {
    pub source: ExecutionId,
    pub target: ExecutionId,
    pub edge_type: EdgeType,
}

impl core::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let value = match self {
            NodeStatus::Queued => "queued",
            NodeStatus::Running => "running",
            NodeStatus::Completed => "completed",
            NodeStatus::Failed => "failed",
        };
        write!(f, "{value}")
    }
}
