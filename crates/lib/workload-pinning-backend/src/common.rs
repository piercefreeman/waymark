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

/// Shared Instance ID type used across all backend traits.
pub trait HasInstanceId {
    /// The Instance ID used by this backend.
    type InstanceId;
}

/// The pinning to apply.
#[derive(Clone, Debug)]
pub struct Pinning<NodeId, Timestamp> {
    /// The id of the node owning this pinning.
    pub node_id: NodeId,

    /// Timestamp at which this claim stops being valid unless refreshed.
    pub expires_at: Timestamp,
}

/// The status of the instance pinning.
#[derive(Clone, Debug)]
pub struct PinningStatus<InstanceId, Pinning> {
    /// Instance this pinning status is for.
    pub instance_id: InstanceId,

    /// Current pinning after the operation, or `None` if the instance
    /// is unpinned.
    pub pinning: Option<Pinning>,
}

/// Shorthand for a [`Pinning`] using the associated types of `T`.
pub type PinningFor<T> =
    Pinning<<T as crate::HasNodeId>::NodeId, <T as crate::HasTimestamp>::Timestamp>;

/// Shorthand for a [`PinningStatus`] using the associated types of `T`.
pub type PinningStatusFor<T> =
    PinningStatus<<T as crate::HasInstanceId>::InstanceId, PinningFor<T>>;
