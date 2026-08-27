//! Common types shared by the scheduler backend traits.

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
