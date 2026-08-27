//! Schedule status update (pause/resume) trait.

use waymark_scheduler_core::ScheduleStatus;

/// Backend capability for updating a schedule's lifecycle status.
pub trait UpdateScheduleStatus {
    /// The error type for status update operations.
    type Error: std::fmt::Debug;

    /// Set the status of the schedule named `schedule_name`.
    ///
    /// Returns `false` when no such schedule exists.
    fn update_schedule_status<'a>(
        &'a self,
        schedule_name: &'a str,
        status: ScheduleStatus,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send + 'a;
}
