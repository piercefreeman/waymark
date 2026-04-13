// The models that we use for our backends are similar to the ones that we
// have specified in our database/Postgres backend, but not 1:1. It's better for
// us to internally convert within the given backend

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use waymark_runner_executor_core::{
    ExecutionException, ExecutionSuccess, UncheckedExecutionResult,
};
use waymark_runner_state::{ExecutionEdge, ExecutionNode, NodeStatus, RunnerState};

use waymark_ids::{ExecutionId, InstanceId, LockId, ScheduleId, WorkflowVersionId};

/// Queued instance payload for the run loop.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuedInstance {
    pub workflow_version_id: WorkflowVersionId,
    #[serde(default)]
    pub schedule_id: Option<ScheduleId>,
    pub entry_node: ExecutionId,
    pub state: RunnerState,
    #[serde(
        default = "HashMap::new",
        deserialize_with = "deserialize_action_results"
    )]
    pub action_results: HashMap<ExecutionId, UncheckedExecutionResult>,
    #[serde(default = "InstanceId::new_uuid_v4")]
    pub instance_id: InstanceId,
    #[serde(default)]
    pub scheduled_at: Option<DateTime<Utc>>,
}

/// Lock claim settings for owned instances.
#[derive(Clone, Debug)]
pub struct LockClaim {
    pub lock_uuid: LockId,
    pub lock_expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
/// Current lock status for an instance.
pub struct InstanceLockStatus {
    pub instance_id: InstanceId,
    pub lock_uuid: Option<LockId>,
    pub lock_expires_at: Option<DateTime<Utc>>,
}

/// Completed instance payload with result or exception.
#[derive(Clone, Debug)]
pub struct InstanceDone {
    pub executor_id: InstanceId,
    pub entry_node: ExecutionId,
    pub result: Option<ExecutionSuccess>,
    pub error: Option<ExecutionException>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Batch payload representing an updated execution graph snapshot.
///
/// This intentionally stores only runtime nodes and edges (no DAG template or
/// derived caches) so persistence stays lightweight.
pub struct GraphUpdate {
    pub instance_id: InstanceId,
    pub nodes: HashMap<ExecutionId, ExecutionNode>,
    pub edges: HashSet<ExecutionEdge>,
}

impl GraphUpdate {
    pub fn from_state(instance_id: InstanceId, state: &RunnerState) -> Self {
        Self {
            instance_id,
            nodes: state.nodes.clone(),
            edges: state.edges.clone(),
        }
    }

    pub fn next_scheduled_at(&self) -> DateTime<Utc> {
        let mut next: Option<DateTime<Utc>> = None;
        for node in self.nodes.values() {
            if matches!(node.status, NodeStatus::Completed | NodeStatus::Failed) {
                continue;
            }
            if let Some(scheduled_at) = node.scheduled_at {
                next = Some(match next {
                    Some(existing) => existing.min(scheduled_at),
                    None => scheduled_at,
                });
            }
        }
        next.unwrap_or_else(Utc::now)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Batch payload representing a finished action attempt (success or failure).
pub struct ActionDone {
    pub execution_id: ExecutionId,
    pub attempt: i32,
    pub status: ActionAttemptStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub result: UncheckedExecutionResult,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionAttemptStatus {
    Completed,
    Failed,
    TimedOut,
}

impl std::fmt::Display for ActionAttemptStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::TimedOut => write!(f, "timed_out"),
        }
    }
}

fn deserialize_action_results<'de, D, ExecutionResult>(
    deserializer: D,
) -> Result<HashMap<ExecutionId, ExecutionResult>, D::Error>
where
    D: serde::Deserializer<'de>,
    ExecutionResult: for<'de1> serde::Deserialize<'de1>,
{
    let value = Option::<HashMap<ExecutionId, ExecutionResult>>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}
