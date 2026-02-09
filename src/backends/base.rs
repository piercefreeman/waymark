//! Backend interfaces for persisting runner state and action results.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use tonic::async_trait;
use uuid::Uuid;

use crate::scheduler::{CreateScheduleParams, ScheduleId, WorkflowSchedule};
use crate::waymark_core::dag::DAG;
use crate::waymark_core::runner::state::{ExecutionEdge, ExecutionNode, NodeStatus, RunnerState};
use crate::webapp::{
    ExecutionGraphView, InstanceDetail, InstanceSummary, ScheduleDetail, ScheduleInvocationSummary,
    ScheduleSummary, TimelineEntry, WorkerActionRow, WorkerAggregateStats, WorkerStatus,
};

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

pub type BackendResult<T> = Result<T, BackendError>;

fn default_instance_id() -> Uuid {
    Uuid::new_v4()
}

fn default_action_results() -> HashMap<Uuid, Value> {
    HashMap::new()
}

fn deserialize_action_results<'de, D>(deserializer: D) -> Result<HashMap<Uuid, Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<HashMap<Uuid, Value>>::deserialize(deserializer)?;
    Ok(value.unwrap_or_default())
}

// The models that we use for our backends are similar to the ones that we
// have specified in our database/Postgres backend, but not 1:1. It's better for
// us to internally convert within the given backend

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
    pub action_results: HashMap<Uuid, Value>,
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
    pub result: Option<Value>,
    pub error: Option<Value>,
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
    pub result: Value,
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

/// Worker status update for persistence.
#[derive(Clone, Debug)]
pub struct WorkerStatusUpdate {
    pub pool_id: Uuid,
    pub throughput_per_min: f64,
    pub total_completed: i64,
    pub last_action_at: Option<chrono::DateTime<chrono::Utc>>,
    pub median_dequeue_ms: Option<i64>,
    pub median_handling_ms: Option<i64>,
    pub dispatch_queue_size: i64,
    pub total_in_flight: i64,
    pub active_workers: i32,
    pub actions_per_sec: f64,
    pub median_instance_duration_secs: Option<f64>,
    pub active_instance_count: i32,
    pub total_instances_completed: i64,
    pub instances_per_sec: f64,
    pub instances_per_min: f64,
    pub time_series: Option<Vec<u8>>,
}

/// Backend capability for recording worker status metrics.
#[async_trait]
pub trait WorkerStatusBackend: Send + Sync {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()>;
}

/// Abstract persistence backend for runner state.
#[async_trait]
pub trait CoreBackend: Send + Sync {
    fn clone_box(&self) -> Box<dyn CoreBackend>;

    /// Persist updated execution graphs.
    async fn save_graphs(
        &self,
        lock_uuid: Uuid,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>>;

    /// Persist finished action attempts (success or failure).
    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()>;

    /// Return up to size queued instances without blocking.
    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch>;

    /// Refresh lock expiry for owned instances.
    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>>;

    /// Release instance locks when evicting from memory.
    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()>;

    /// Persist completed workflow instances.
    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()>;

    /// Insert queued instances for run-loop consumption.
    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()>;
}

/// Registration payload for storing workflow DAG metadata.
#[derive(Clone, Debug)]
pub struct WorkflowRegistration {
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

#[derive(Clone, Debug)]
/// Stored workflow version metadata and IR payload.
pub struct WorkflowVersion {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_version: String,
    pub ir_hash: String,
    pub program_proto: Vec<u8>,
    pub concurrent: bool,
}

/// Backend capability for registering workflow DAGs.
#[async_trait]
pub trait WorkflowRegistryBackend: Send + Sync {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid>;

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>>;
}

/// Backend capability for workflow schedule persistence.
#[async_trait]
pub trait SchedulerBackend: Send + Sync {
    async fn upsert_schedule(&self, params: &CreateScheduleParams) -> BackendResult<ScheduleId>;
    async fn get_schedule(&self, id: ScheduleId) -> BackendResult<WorkflowSchedule>;
    async fn get_schedule_by_name(
        &self,
        workflow_name: &str,
        schedule_name: &str,
    ) -> BackendResult<Option<WorkflowSchedule>>;
    async fn list_schedules(&self, limit: i64, offset: i64)
    -> BackendResult<Vec<WorkflowSchedule>>;
    async fn count_schedules(&self) -> BackendResult<i64>;
    async fn update_schedule_status(&self, id: ScheduleId, status: &str) -> BackendResult<bool>;
    async fn delete_schedule(&self, id: ScheduleId) -> BackendResult<bool>;
    async fn find_due_schedules(&self, limit: i32) -> BackendResult<Vec<WorkflowSchedule>>;
    async fn has_running_instance(&self, schedule_id: ScheduleId) -> BackendResult<bool>;
    async fn mark_schedule_executed(
        &self,
        schedule_id: ScheduleId,
        instance_id: Uuid,
    ) -> BackendResult<()>;
    async fn skip_schedule_run(&self, schedule_id: ScheduleId) -> BackendResult<()>;
}

#[derive(Clone, Copy, Debug, Default)]
/// Summary of a garbage collection sweep.
pub struct GarbageCollectionResult {
    pub deleted_instances: usize,
    pub deleted_actions: usize,
}

/// Backend capability for deleting old finished workflow data.
#[async_trait]
pub trait GarbageCollectorBackend: Send + Sync {
    async fn collect_done_instances(
        &self,
        older_than: DateTime<Utc>,
        limit: usize,
    ) -> BackendResult<GarbageCollectionResult>;
}

/// Backend capability for webapp-specific queries.
#[async_trait]
pub trait WebappBackend: Send + Sync {
    async fn count_instances(&self, search: Option<&str>) -> BackendResult<i64>;
    async fn list_instances(
        &self,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<InstanceSummary>>;
    async fn get_instance(&self, instance_id: Uuid) -> BackendResult<InstanceDetail>;
    async fn get_execution_graph(
        &self,
        instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>>;
    async fn get_workflow_graph(
        &self,
        instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>>;
    async fn get_action_results(&self, instance_id: Uuid) -> BackendResult<Vec<TimelineEntry>>;
    async fn get_distinct_workflows(&self) -> BackendResult<Vec<String>>;
    async fn get_distinct_statuses(&self) -> BackendResult<Vec<String>>;
    async fn count_schedules(&self) -> BackendResult<i64>;
    async fn list_schedules(&self, limit: i64, offset: i64) -> BackendResult<Vec<ScheduleSummary>>;
    async fn get_schedule(&self, schedule_id: Uuid) -> BackendResult<ScheduleDetail>;
    async fn count_schedule_invocations(&self, schedule_id: Uuid) -> BackendResult<i64>;
    async fn list_schedule_invocations(
        &self,
        schedule_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<ScheduleInvocationSummary>>;
    async fn update_schedule_status(&self, schedule_id: Uuid, status: &str) -> BackendResult<bool>;
    async fn get_distinct_schedule_statuses(&self) -> BackendResult<Vec<String>>;
    async fn get_distinct_schedule_types(&self) -> BackendResult<Vec<String>>;
    async fn get_worker_action_stats(
        &self,
        window_minutes: i64,
    ) -> BackendResult<Vec<WorkerActionRow>>;
    async fn get_worker_aggregate_stats(
        &self,
        window_minutes: i64,
    ) -> BackendResult<WorkerAggregateStats>;
    async fn worker_status_table_exists(&self) -> bool;
    async fn schedules_table_exists(&self) -> bool;
    async fn get_worker_statuses(&self, window_minutes: i64) -> BackendResult<Vec<WorkerStatus>>;
}

impl Clone for Box<dyn CoreBackend> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
