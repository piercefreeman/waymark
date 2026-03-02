// The models that we use for our backends are similar to the ones that we
// have specified in our database/Postgres backend, but not 1:1. It's better for
// us to internally convert within the given backend

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use waymark_dag::DAG;
use waymark_runner_state::{ExecutionEdge, ExecutionNode, NodeStatus, RunnerState};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Queued instance payload for the run loop.
pub struct QueuedInstance {
    pub workflow_version_id: Uuid,
    #[serde(default)]
    pub schedule_id: Option<Uuid>,
    #[serde(skip, default)]
    pub dag: Option<Arc<DAG>>,
    pub entry_node: Uuid,
    pub state: Option<RunnerState>,
    #[serde(
        default = "default_action_results",
        deserialize_with = "deserialize_action_results"
    )]
    pub action_results: HashMap<Uuid, serde_json::Value>,
    #[serde(default = "default_instance_id")]
    pub instance_id: Uuid,
    #[serde(default)]
    pub scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
/// Result payload for queued instance polling.
pub struct QueuedInstanceBatch {
    pub instances: Vec<QueuedInstance>,
}

#[derive(Clone, Debug)]
/// Lock claim settings for owned instances.
pub struct LockClaim {
    pub lock_uuid: Uuid,
    pub lock_expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
/// Current lock status for an instance.
pub struct InstanceLockStatus {
    pub instance_id: Uuid,
    pub lock_uuid: Option<Uuid>,
    pub lock_expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Completed instance payload with result or exception.
pub struct InstanceDone {
    pub executor_id: Uuid,
    pub entry_node: Uuid,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Batch payload representing an updated execution graph snapshot.
///
/// This intentionally stores only runtime nodes and edges (no DAG template or
/// derived caches) so persistence stays lightweight.
pub struct GraphUpdate {
    pub instance_id: Uuid,
    pub nodes: HashMap<Uuid, ExecutionNode>,
    pub edges: HashSet<ExecutionEdge>,
}

impl GraphUpdate {
    pub fn from_state(instance_id: Uuid, state: &RunnerState) -> Self {
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
    pub execution_id: Uuid,
    pub attempt: i32,
    pub status: ActionAttemptStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub duration_ms: Option<i64>,
    pub result: serde_json::Value,
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

fn default_instance_id() -> Uuid {
    Uuid::new_v4()
}

fn default_action_results() -> HashMap<Uuid, serde_json::Value> {
    HashMap::new()
}

fn deserialize_action_results<'de, D>(
    deserializer: D,
) -> Result<HashMap<Uuid, serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<HashMap<Uuid, serde_json::Value>>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}
