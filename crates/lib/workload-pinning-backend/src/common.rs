//! Common types shared by all workload pinning backend traits.

/// Shared time representation used across all backend traits.
pub trait HasTimestamp {
    /// The time representation used by this backend.
    type Timestamp;
}

/// Shared Node ID type used across all backend traits.
pub trait HasNodeId {
    /// The Node ID used by this backend.
    type NodeId;
}

/// Shared Workload ID type used across all backend traits.
pub trait HasWorkloadId {
    /// The Workload ID used by this backend.
    type WorkloadId;
}

/// A node's hold on a workload.
#[derive(Clone, Debug)]
pub struct Pinning<NodeId, Timestamp> {
    /// The id of the node owning this pinning.
    pub node_id: NodeId,

    /// Timestamp at which this pinning stops being valid unless refreshed.
    pub expires_at: Timestamp,
}

/// The status of the workload pinning.
#[derive(Clone, Debug)]
pub struct PinningStatus<WorkloadId, Pinning> {
    /// Workload this pinning status is for.
    pub workload_id: WorkloadId,

    /// Current pinning after the operation, or `None` if the operating
    /// node no longer holds a pinning on the workload.
    pub pinning: Option<Pinning>,
}

/// Shorthand for a [`Pinning`] using the associated types of `T`.
pub type PinningFor<T> =
    Pinning<<T as crate::HasNodeId>::NodeId, <T as crate::HasTimestamp>::Timestamp>;

/// Shorthand for a [`PinningStatus`] using the associated types of `T`.
pub type PinningStatusFor<T> =
    PinningStatus<<T as crate::HasWorkloadId>::WorkloadId, PinningFor<T>>;
