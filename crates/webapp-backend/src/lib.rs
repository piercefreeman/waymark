use uuid::Uuid;
pub use waymark_backends_core::{BackendError, BackendResult};
use waymark_webapp_core::{
    ExecutionGraphView, InstanceDetail, InstanceSummary, ScheduleDetail, ScheduleInvocationSummary,
    ScheduleSummary, TimelineEntry, WorkerActionRow, WorkerAggregateStats, WorkerStatus,
};

/// Backend capability for webapp-specific queries.
#[async_trait::async_trait]
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
