use uuid::Uuid;

pub use waymark_backends_core::{BackendError, BackendResult};
use waymark_scheduler_core::{CreateScheduleParams, ScheduleId, WorkflowSchedule};

/// Backend capability for workflow schedule persistence.
#[async_trait::async_trait]
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
