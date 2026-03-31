use std::future::Future;

use uuid::Uuid;
use waymark_backends_core::BackendResult;
use waymark_ids::InstanceId;
use waymark_webapp_core::{
    ExecutionGraphView, InstanceDetail, InstanceSummary, ScheduleDetail, ScheduleInvocationSummary,
    ScheduleSummary, TimelineEntry, WorkerActionRow, WorkerAggregateStats, WorkerStatus,
};

/// Backend capability for webapp-specific queries.
pub trait WebappBackend {
    fn count_instances<'a>(
        &'a self,
        search: Option<&'a str>,
    ) -> impl Future<Output = BackendResult<i64>> + Send + 'a;

    fn list_instances<'a>(
        &'a self,
        search: Option<&'a str>,
        limit: i64,
        offset: i64,
    ) -> impl Future<Output = BackendResult<Vec<InstanceSummary>>> + Send + 'a;

    fn get_instance(
        &self,
        instance_id: InstanceId,
    ) -> impl Future<Output = BackendResult<InstanceDetail>> + Send + '_;

    fn get_execution_graph(
        &self,
        instance_id: InstanceId,
    ) -> impl Future<Output = BackendResult<Option<ExecutionGraphView>>> + Send + '_;

    fn requeue_instance_to_latest_version(
        &self,
        instance_id: InstanceId,
    ) -> impl Future<Output = BackendResult<InstanceId>> + Send + '_;

    fn get_workflow_graph(
        &self,
        instance_id: InstanceId,
    ) -> impl Future<Output = BackendResult<Option<ExecutionGraphView>>> + Send + '_;

    fn get_action_results(
        &self,
        instance_id: InstanceId,
    ) -> impl Future<Output = BackendResult<Vec<TimelineEntry>>> + Send + '_;

    fn get_distinct_workflows(
        &self,
    ) -> impl Future<Output = BackendResult<Vec<String>>> + Send + '_;

    fn get_distinct_statuses(&self)
    -> impl Future<Output = BackendResult<Vec<String>>> + Send + '_;

    fn count_schedules(&self) -> impl Future<Output = BackendResult<i64>> + Send + '_;

    fn list_schedules(
        &self,
        limit: i64,
        offset: i64,
    ) -> impl Future<Output = BackendResult<Vec<ScheduleSummary>>> + Send + '_;

    fn get_schedule(
        &self,
        schedule_id: Uuid,
    ) -> impl Future<Output = BackendResult<ScheduleDetail>> + Send + '_;

    fn count_schedule_invocations(
        &self,
        schedule_id: Uuid,
    ) -> impl Future<Output = BackendResult<i64>> + Send + '_;

    fn list_schedule_invocations(
        &self,
        schedule_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> impl Future<Output = BackendResult<Vec<ScheduleInvocationSummary>>> + Send + '_;

    fn update_schedule_status<'a>(
        &'a self,
        schedule_id: Uuid,
        status: &'a str,
    ) -> impl Future<Output = BackendResult<bool>> + Send + 'a;

    fn get_distinct_schedule_statuses(
        &self,
    ) -> impl Future<Output = BackendResult<Vec<String>>> + Send + '_;

    fn get_distinct_schedule_types(
        &self,
    ) -> impl Future<Output = BackendResult<Vec<String>>> + Send + '_;

    fn get_worker_action_stats(
        &self,
        window_minutes: i64,
    ) -> impl Future<Output = BackendResult<Vec<WorkerActionRow>>> + Send + '_;

    fn get_worker_aggregate_stats(
        &self,
        window_minutes: i64,
    ) -> impl Future<Output = BackendResult<WorkerAggregateStats>> + Send + '_;

    fn worker_status_table_exists(&self) -> impl Future<Output = bool> + Send + '_;

    fn schedules_table_exists(&self) -> impl Future<Output = bool> + Send + '_;

    fn get_worker_statuses(
        &self,
        window_minutes: i64,
    ) -> impl Future<Output = BackendResult<Vec<WorkerStatus>>> + Send + '_;
}
