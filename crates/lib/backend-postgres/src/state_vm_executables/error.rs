//! Error types for the state-vm-executables postgres backend.

use waymark_ids::WorkflowVersionId;

/// Error returned when loading a compiled executable.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),

    /// No executable found with the given id.
    #[error("executable not found: {0}")]
    NotFound(WorkflowVersionId),
}
