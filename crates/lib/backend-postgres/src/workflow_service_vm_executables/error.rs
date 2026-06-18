//! Error types for the workflow-service-vm-executables postgres backend.

/// Error returned when storing a compiled executable.
#[derive(Debug, thiserror::Error)]
pub enum UpsertError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// A conflicting executable already exists.
    #[error("executable conflict")]
    Conflict,
}

impl waymark_workflow_service_vm_executables_backend::Error for UpsertError {
    fn kind(&self) -> waymark_workflow_service_vm_executables_backend::ErrorKind {
        match self {
            Self::Sqlx(_) => waymark_workflow_service_vm_executables_backend::ErrorKind::Internal,
            Self::Conflict => waymark_workflow_service_vm_executables_backend::ErrorKind::Conflict,
        }
    }
}
