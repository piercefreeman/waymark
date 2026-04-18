use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum NodeStatus {
    Queued,
    Running,
    Completed,
    Failed,
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
