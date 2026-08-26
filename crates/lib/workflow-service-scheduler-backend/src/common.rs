//! Common types shared by the workflow-service scheduler backend traits.

use waymark_scheduler_core::ScheduleStatus;

/// Common base: the backend is associated with an executable identifier
/// type.
pub trait HasExecutableId {
    /// The executable / workflow version identifier type.
    type ExecutableId;
}

/// Common base: the backend is associated with a VM identifier type.
pub trait HasVmId {
    /// The VM / workflow instance identifier type.
    type VmId;
}

/// Common base: the backend is associated with a timestamp type.
pub trait HasTimestamp {
    /// The timestamp type for schedule run cursors.
    type Timestamp;
}

/// Shorthand for a [`ScheduleRecord`] using the associated types of `T`.
pub type ScheduleRecordFor<T> =
    ScheduleRecord<<T as crate::HasVmId>::VmId, <T as crate::HasTimestamp>::Timestamp>;

/// A schedule row as read back for get/list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRecord<VmId, Timestamp> {
    /// The schedule's name — its sole key.
    pub schedule_name: String,

    /// The pinned executable's workflow name, from the executables
    /// store; it is not stored on the schedule row.
    pub workflow_name: String,

    /// The definition blob, unparsed — decoded by the service layer.
    pub definition: Vec<u8>,

    /// The schedule's lifecycle status.
    pub status: ScheduleStatus,

    /// When the next run is due.
    pub next_run_at: Timestamp,

    /// The most recently spawned instance, if any run was ever spawned.
    pub last_instance_id: Option<VmId>,
}
