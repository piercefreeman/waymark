//! Errors returned by manager operations.

/// Error returned by [`super::VmRuntimesManager::spawn`].
#[derive(Debug, thiserror::Error)]
pub enum SpawnError<VmId>
where
    VmId: core::fmt::Debug,
{
    /// The VM id is already in use.
    #[error("VM {0:?} is already running")]
    AlreadyRunning(VmId),
}

/// Error returned by [`super::VmRuntimesManager::revive`].
#[derive(Debug, thiserror::Error)]
pub enum ReviveError<VmId, LoadError, DeserializeError>
where
    VmId: core::fmt::Debug,
    DeserializeError: std::error::Error + 'static,
{
    /// Load operation failed.
    #[error("load: {0}")]
    Load(#[source] LoadError),

    /// Failed to deserialize a snapshot from the backend.
    #[error("deserialization failed for VM {vm_id:?}")]
    DeserializationFailed {
        /// The VM id.
        vm_id: VmId,

        /// The underlying deserialization error.
        #[source]
        error: DeserializeError,
    },

    /// Spawn failed (e.g., the VM is already running).
    #[error(transparent)]
    Spawn(SpawnError<VmId>),
}
