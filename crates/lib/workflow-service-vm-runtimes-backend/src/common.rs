//! Shared identifier traits for workflow-service backends.

/// Common base: every workflow-service backend is associated with a VM
/// identifier type.
pub trait HasVmId {
    /// The VM / workflow identifier type.
    type VmId;
}

/// Associates the backend with an executable (workflow version) identifier
/// type.
pub trait HasExecutableId {
    /// The executable / workflow version identifier type.
    type ExecutableId;
}
