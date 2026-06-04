/// A lifecycle action to take for a VM.
///
/// Returned by [`LifecyclePolicy::evaluate`](crate::LifecyclePolicy::evaluate)
/// after inspecting the current [`VmState`](crate::VmState).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleDecision {
    /// No action is needed — the VM can continue running as-is.
    NoAction,

    /// The VM should be persisted (snapshotted) at the next opportunity.
    ///
    /// This does not force an immediate snapshot; the caller decides when
    /// to act on the recommendation.
    Persist,

    /// The VM should be evicted from memory.
    ///
    /// The caller is expected to snapshot the VM (if needed) before
    /// dropping it.
    Evict,
}

impl LifecycleDecision {
    /// Returns `true` if this decision requires a snapshot.
    pub fn should_persist(self) -> bool {
        matches!(self, Self::Persist | Self::Evict)
    }

    /// Returns `true` if this decision requires eviction.
    pub fn should_evict(self) -> bool {
        matches!(self, Self::Evict)
    }
}
