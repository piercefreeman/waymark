use serde::{Deserialize, Serialize};
use waymark_dag::EdgeType;
use waymark_ids::ExecutionId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionEdge {
    pub source: ExecutionId,
    pub target: ExecutionId,
    pub edge_type: EdgeType,
}
