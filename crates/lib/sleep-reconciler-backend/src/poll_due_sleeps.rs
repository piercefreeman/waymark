//! Demand-driven due-sleep polling trait.

use nonempty_collections::NESlice;

use super::common::{HasTimestamp, HasVmId, SleepKey};

/// Backend capability for polling recorded sleeps by demand and dueness.
pub trait PollDueSleeps: HasVmId + HasTimestamp {
    /// The error type for poll operations.
    type Error: core::fmt::Debug;

    /// Fetch the recorded sleeps matching the demanded keys whose
    /// deadline has passed.
    ///
    /// Returns only keys that are in `demand` and whose `wake_at` is at
    /// or before `now`; an empty result means none of the demanded sleeps
    /// are recorded and due yet, which is normal — the caller polls
    /// again.
    fn poll_due_sleeps<'a>(
        &'a self,
        now: Self::Timestamp,
        demand: NESlice<'a, SleepKey<Self::VmId>>,
    ) -> impl Future<Output = Result<Vec<SleepKey<Self::VmId>>, Self::Error>> + Send + 'a;
}
