use serde::{Deserialize, Serialize};
use waymark_ids::ExecutionId;

use crate::ExecutionGraph;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionState {
    pub graph: ExecutionGraph,
    pub current_node: ExecutionId,
}
