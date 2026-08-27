//! Due-schedule polling trait.

use std::num::NonZeroUsize;

use nonempty_collections::NEVec;

use super::common::{HasTimestamp, HasVmId};

/// A due schedule row as fetched for spawning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueSchedule<VmId, Timestamp> {
    /// The schedule's name — its sole key.
    pub schedule_name: String,

    /// The definition blob, unparsed — decoded by the scheduler loop.
    pub definition: Vec<u8>,

    /// When this occurrence was due. Doubles as the fence for the
    /// registration that spawns it.
    pub next_run_at: Timestamp,

    /// The most recently spawned instance, if any run was ever spawned.
    pub last_instance_id: Option<VmId>,
}

/// Shorthand for a [`DueSchedule`] using the associated types of `T`.
pub type DueScheduleFor<T> = DueSchedule<<T as HasVmId>::VmId, <T as HasTimestamp>::Timestamp>;

/// Backend capability for polling due schedules.
pub trait PollDueSchedules: HasVmId + HasTimestamp {
    /// An error that can occur while polling.
    type Error: std::fmt::Debug;

    /// Return up to `max_items` due schedules without blocking: active
    /// schedules whose run cursor is at or before `now`. A plain read —
    /// claiming happens at registration via the fence.
    ///
    /// Returns `Ok(None)` if no schedules are due.
    fn poll_due_schedules(
        &self,
        now: Self::Timestamp,
        max_items: NonZeroUsize,
    ) -> impl Future<Output = Result<Option<NEVec<DueScheduleFor<Self>>>, Self::Error>> + Send + '_;
}
