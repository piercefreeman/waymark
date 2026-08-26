//! Schedule listing trait.

use waymark_scheduler_core::ScheduleStatus;

use super::common::{HasTimestamp, HasVmId, ScheduleRecordFor};

/// Backend capability for listing schedules.
pub trait ListSchedules: HasVmId + HasTimestamp {
    /// The error type for list operations.
    type Error: std::fmt::Debug;

    /// Fetch all schedules, or only those with the given status.
    fn list_schedules(
        &self,
        status: Option<ScheduleStatus>,
    ) -> impl Future<Output = Result<Vec<ScheduleRecordFor<Self>>, Self::Error>> + Send + '_;
}
