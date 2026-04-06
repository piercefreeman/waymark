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

use waymark_ids::{ExecutionId, InstanceId, LockId};

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Queued instance payload for the run loop.
pub struct QueuedInstance {
    pub workflow_version_id: Uuid,
    #[serde(default)]
    pub schedule_id: Option<Uuid>,
    #[serde(skip, default)]
    pub dag: Option<Arc<DAG>>,
    pub entry_node: ExecutionId,
    pub state: Option<RunnerState>,
    #[serde(
        default = "HashMap::new",
        deserialize_with = "deserialize_action_results"
    )]
    pub action_results: HashMap<ExecutionId, serde_json::Value>,
    #[serde(default = "InstanceId::new_uuid_v4")]
    pub instance_id: InstanceId,
    #[serde(default)]
    pub scheduled_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
/// Lock claim settings for owned instances.
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

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Completed instance payload with result or exception.
pub struct InstanceDone {
    pub executor_id: InstanceId,
    pub entry_node: ExecutionId,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
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

    pub fn from_durable_state(instance_id: InstanceId, state: &RunnerState) -> Self {
        let durable_node_ids: HashSet<ExecutionId> = state
            .nodes
            .iter()
            .filter_map(|(node_id, node)| {
                (node.is_action_call() || node.is_sleep()).then_some(*node_id)
            })
            .collect();

        let nodes = state
            .nodes
            .iter()
            .filter(|(node_id, _)| durable_node_ids.contains(*node_id))
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect();
        let edges = state
            .edges
            .iter()
            .filter(|edge| {
                durable_node_ids.contains(&edge.source) || durable_node_ids.contains(&edge.target)
            })
            .cloned()
            .collect();

        Self {
            instance_id,
            nodes,
            edges,
        }
    }

    pub fn merge_onto_base_state(&self, base_state: Option<&RunnerState>) -> RunnerState {
        let mut nodes = base_state
            .map(|state| state.nodes.clone())
            .unwrap_or_default();
        nodes.extend(self.nodes.clone());

        let mut edges = base_state
            .map(|state| state.edges.clone())
            .unwrap_or_default();
        edges.extend(self.edges.clone());

        RunnerState::new(None, Some(nodes), Some(edges), false)
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

fn deserialize_action_results<'de, D>(
    deserializer: D,
) -> Result<HashMap<ExecutionId, serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<HashMap<ExecutionId, serde_json::Value>>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use chrono::Duration;
    use serde_json::json;
    use waymark_dag::EdgeType;
    use waymark_runner_state::value_visitor::ValueExpr;
    use waymark_runner_state::{ActionCallSpec, ExecutionEdge, ExecutionNode, LiteralValue};

    use super::*;

    fn action_node(node_id: ExecutionId) -> ExecutionNode {
        ExecutionNode {
            node_id,
            node_type: "action_call".to_string(),
            label: "@tests.work()".to_string(),
            status: NodeStatus::Running,
            template_id: Some("action".to_string()),
            targets: vec!["result".to_string()],
            action: Some(ActionCallSpec {
                action_name: "tests.work".to_string(),
                module_name: Some("tests".to_string()),
                kwargs: HashMap::new(),
            }),
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: 2,
            started_at: Some(Utc::now()),
            completed_at: None,
            scheduled_at: None,
        }
    }

    fn sleep_node(node_id: ExecutionId) -> ExecutionNode {
        ExecutionNode {
            node_id,
            node_type: "sleep".to_string(),
            label: "sleep(5)".to_string(),
            status: NodeStatus::Queued,
            template_id: Some("sleep".to_string()),
            targets: Vec::new(),
            action: None,
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: 0,
            started_at: None,
            completed_at: None,
            scheduled_at: Some(Utc::now() + Duration::seconds(5)),
        }
    }

    fn assignment_node(node_id: ExecutionId, label: &str, target: &str) -> ExecutionNode {
        ExecutionNode {
            node_id,
            node_type: "assignment".to_string(),
            label: label.to_string(),
            status: NodeStatus::Completed,
            template_id: Some("assign".to_string()),
            targets: vec![target.to_string()],
            action: None,
            value_expr: None,
            assignments: HashMap::from([(
                target.to_string(),
                ValueExpr::Literal(LiteralValue { value: json!(1) }),
            )]),
            action_attempt: 0,
            started_at: None,
            completed_at: Some(Utc::now()),
            scheduled_at: None,
        }
    }

    #[test]
    fn graph_update_from_durable_state_filters_inline_nodes() {
        let instance_id = InstanceId::new_uuid_v4();
        let input_node_id = ExecutionId::new_uuid_v4();
        let action_node_id = ExecutionId::new_uuid_v4();
        let assignment_node_id = ExecutionId::new_uuid_v4();
        let sleep_node_id = ExecutionId::new_uuid_v4();

        let state = RunnerState::new(
            None,
            Some(HashMap::from([
                (
                    input_node_id,
                    assignment_node(input_node_id, "input x = 1", "x"),
                ),
                (action_node_id, action_node(action_node_id)),
                (
                    assignment_node_id,
                    assignment_node(assignment_node_id, "result = 1", "result"),
                ),
                (sleep_node_id, sleep_node(sleep_node_id)),
            ])),
            Some(HashSet::from([
                ExecutionEdge {
                    source: input_node_id,
                    target: action_node_id,
                    edge_type: EdgeType::StateMachine,
                },
                ExecutionEdge {
                    source: action_node_id,
                    target: assignment_node_id,
                    edge_type: EdgeType::StateMachine,
                },
                ExecutionEdge {
                    source: assignment_node_id,
                    target: sleep_node_id,
                    edge_type: EdgeType::StateMachine,
                },
                ExecutionEdge {
                    source: input_node_id,
                    target: assignment_node_id,
                    edge_type: EdgeType::DataFlow,
                },
            ])),
            false,
        );

        let graph = GraphUpdate::from_durable_state(instance_id, &state);

        assert_eq!(graph.instance_id, instance_id);
        assert_eq!(
            graph.nodes.keys().copied().collect::<HashSet<_>>(),
            HashSet::from([action_node_id, sleep_node_id]),
        );
        assert_eq!(
            graph.edges,
            HashSet::from([
                ExecutionEdge {
                    source: input_node_id,
                    target: action_node_id,
                    edge_type: EdgeType::StateMachine,
                },
                ExecutionEdge {
                    source: action_node_id,
                    target: assignment_node_id,
                    edge_type: EdgeType::StateMachine,
                },
                ExecutionEdge {
                    source: assignment_node_id,
                    target: sleep_node_id,
                    edge_type: EdgeType::StateMachine,
                },
            ]),
        );
    }

    #[test]
    fn graph_update_merge_onto_base_state_preserves_bootstrap_nodes() {
        let entry_node_id = ExecutionId::new_uuid_v4();
        let action_node_id = ExecutionId::new_uuid_v4();
        let base_state = RunnerState::new(
            None,
            Some(HashMap::from([(
                entry_node_id,
                assignment_node(entry_node_id, "input value = 1", "value"),
            )])),
            Some(HashSet::new()),
            false,
        );
        let graph = GraphUpdate {
            instance_id: InstanceId::new_uuid_v4(),
            nodes: HashMap::from([(action_node_id, action_node(action_node_id))]),
            edges: HashSet::from([ExecutionEdge {
                source: entry_node_id,
                target: action_node_id,
                edge_type: EdgeType::StateMachine,
            }]),
        };

        let merged = graph.merge_onto_base_state(Some(&base_state));

        assert!(merged.nodes.contains_key(&entry_node_id));
        assert!(merged.nodes.contains_key(&action_node_id));
        assert!(merged.edges.contains(&ExecutionEdge {
            source: entry_node_id,
            target: action_node_id,
            edge_type: EdgeType::StateMachine,
        }));
    }
}
