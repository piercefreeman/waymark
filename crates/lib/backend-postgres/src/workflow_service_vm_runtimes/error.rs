//! Error types for the workflow-service postgres backend.

/// Error returned by [`super::PostgresBackend`] when registering VM
/// runtimes via the trait.
///
/// Already-registered VM runtimes are not errors — they are reported via
/// [`waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::SomeAlreadyRegistered`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterVmRuntimesError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}

/// Error returned by [`super::PostgresBackend`] when querying which VM
/// runtimes are registered.
#[derive(Debug, thiserror::Error)]
pub enum FindExistingVmRuntimesError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),
}
