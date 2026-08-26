//! Single-schedule read trait.

use super::common::{HasTimestamp, HasVmId, ScheduleRecordFor};

/// Backend capability for reading one schedule by name.
pub trait GetSchedule: HasVmId + HasTimestamp {
    /// The error type for read operations.
    type Error: std::fmt::Debug;

    /// Fetch the schedule named `schedule_name`, or `None` if no such
    /// schedule exists.
    fn get_schedule<'a>(
        &'a self,
        schedule_name: &'a str,
    ) -> impl Future<Output = Result<Option<ScheduleRecordFor<Self>>, Self::Error>> + Send + 'a;
}
