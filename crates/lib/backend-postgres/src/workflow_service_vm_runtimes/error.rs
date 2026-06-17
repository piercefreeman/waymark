//! Error types for the workflow-service postgres backend.

use waymark_ids::InstanceId;
use waymark_workflow_service_vm_runtimes_backend::register_vm_runtime;

/// Error returned by [`super::PostgresBackend`] when registering a VM runtime
/// via the trait.
#[derive(Debug, thiserror::Error)]
pub enum RegisterVmRuntimeError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// A runtime is already registered for this VM.
    #[error("vm runtime already registered: {0}")]
    AlreadyExists(InstanceId),
}

impl register_vm_runtime::Error for RegisterVmRuntimeError {
    fn kind(&self) -> register_vm_runtime::ErrorKind {
        match self {
            Self::Sqlx(_) => register_vm_runtime::ErrorKind::Internal,
            Self::AlreadyExists(_) => register_vm_runtime::ErrorKind::AlreadyRegistered,
        }
    }
}
