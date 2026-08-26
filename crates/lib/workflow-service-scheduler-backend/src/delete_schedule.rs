//! Schedule deletion trait.

/// Backend capability for deleting a schedule.
pub trait DeleteSchedule {
    /// The error type for delete operations.
    type Error: std::fmt::Debug;

    /// Hard-delete the schedule named `schedule_name`. Already-spawned
    /// instances are unaffected.
    ///
    /// Returns `false` when no such schedule exists.
    fn delete_schedule<'a>(
        &'a self,
        schedule_name: &'a str,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send + 'a;
}
