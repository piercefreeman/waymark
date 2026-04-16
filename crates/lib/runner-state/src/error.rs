use waymark_ids::ExecutionId;

use crate::max_nodes::MAX_RUNNER_STATE_NODES;

/// Raised when the runner state cannot be updated safely.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RunnerStateError(pub String);

impl RunnerStateError {
    const NODE_LIMIT_EXCEEDED_PREFIX: &str = "runner state node limit exceeded:";

    pub fn node_limit_exceeded(node_id: ExecutionId, existing_nodes: usize) -> Self {
        Self(format!(
            "runner state node limit exceeded: attempted to queue node {} with {} existing nodes (max {})",
            node_id, existing_nodes, *MAX_RUNNER_STATE_NODES,
        ))
    }

    pub fn is_node_limit_exceeded(&self) -> bool {
        Self::is_node_limit_exceeded_message(&self.0)
    }

    pub fn is_node_limit_exceeded_message(message: &str) -> bool {
        message.starts_with(Self::NODE_LIMIT_EXCEEDED_PREFIX)
    }
}
