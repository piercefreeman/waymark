use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use waymark_ids::ExecutionId;
use waymark_runner_expr::{ActionCallSpec, ValueExpr};

use crate::{ExecutionNodeType, NodeStatus, UnknownExecutionNodeTypeError};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionNode {
    pub node_id: ExecutionId,
    pub node_type: String,
    pub label: String,
    pub status: NodeStatus,
    pub template_id: Option<String>,
    pub targets: Vec<String>,
    pub action: Option<ActionCallSpec<ExecutionId>>,
    pub value_expr: Option<ValueExpr<ExecutionId>>,
    pub assignments: HashMap<String, ValueExpr<ExecutionId>>,
    pub action_attempt: i32,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub scheduled_at: Option<DateTime<Utc>>,
}

impl ExecutionNode {
    pub fn node_type_enum(&self) -> Result<ExecutionNodeType, UnknownExecutionNodeTypeError> {
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
