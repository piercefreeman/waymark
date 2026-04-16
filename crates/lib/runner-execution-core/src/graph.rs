use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use waymark_ids::ExecutionId;

use crate::{ExecutionEdge, ExecutionNode};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionGraph {
    pub nodes: HashMap<ExecutionId, ExecutionNode>,
    pub edges: HashSet<ExecutionEdge>,
}
