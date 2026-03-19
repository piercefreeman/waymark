use uuid::Uuid;

pub use waymark_backends_core::{BackendError, BackendResult};
use waymark_scheduler_core::{CreateScheduleParams, ScheduleId, WorkflowSchedule};

/// Backend capability for workflow schedule persistence.
pub trait SchedulerBackend {
    fn upsert_schedule<'a>(
        &'a self,
        params: &'a CreateScheduleParams,
    ) -> impl Future<Output = BackendResult<ScheduleId>> + Send + 'a;

    fn get_schedule(
        &self,
        id: ScheduleId,
    ) -> impl Future<Output = BackendResult<WorkflowSchedule>> + Send + '_;

    fn get_schedule_by_name<'a>(
        &'a self,
        workflow_name: &'a str,
        schedule_name: &'a str,
    ) -> impl Future<Output = BackendResult<Option<WorkflowSchedule>>> + Send + 'a;

    fn list_schedules(
        &self,
        limit: i64,
        offset: i64,
    ) -> impl Future<Output = BackendResult<Vec<WorkflowSchedule>>> + Send + '_;

    fn count_schedules(&self) -> impl Future<Output = BackendResult<i64>> + Send + '_;

    fn update_schedule_status<'a>(
        &'a self,
        id: ScheduleId,
        status: &'a str,
    ) -> impl Future<Output = BackendResult<bool>> + Send + 'a;

    fn delete_schedule(
        &self,
        id: ScheduleId,
    ) -> impl Future<Output = BackendResult<bool>> + Send + '_;

    fn find_due_schedules(
        &self,
        limit: i32,
    ) -> impl Future<Output = BackendResult<Vec<WorkflowSchedule>>> + Send + '_;

    fn has_running_instance(
        &self,
        schedule_id: ScheduleId,
    ) -> impl Future<Output = BackendResult<bool>> + Send + '_;

    fn mark_schedule_executed(
        &self,
        schedule_id: ScheduleId,
        instance_id: Uuid,
    ) -> impl Future<Output = BackendResult<()>> + Send + '_;

    fn skip_schedule_run(
        &self,
        schedule_id: ScheduleId,
    ) -> impl Future<Output = BackendResult<()>> + Send + '_;
}
