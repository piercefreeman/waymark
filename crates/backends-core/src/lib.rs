//! Core primitives for various waymark subsystem backends.

/// The common backend error.
///
/// TODO: move away from a shared notion of backend error to use concrete error
/// type per-operation (rather than per-subsystem or per-crate).
#[derive(Debug, thiserror::Error)]
pub enum BackendError<Inner = InnerError> {
    #[error("{0}")]
    Message(String),

    #[error(transparent)]
    Inner(Inner),

    #[error(transparent)]
    Serialization(serde_json::Error),
}

#[cfg(feature = "sqlx-error")]
pub type InnerError = sqlx::Error;

#[cfg(not(feature = "sqlx-error"))]
pub type InnerError = ();

/// Utility type alias for backend results.
///
/// TODO: move away from the single-`Result` type aliases as we want to vary
/// rrors per-call.
pub type BackendResult<T, E = InnerError> = Result<T, BackendError<E>>;

#[cfg(feature = "sqlx-error")]
impl From<sqlx::Error> for BackendError<sqlx::Error> {
    fn from(value: sqlx::Error) -> Self {
        Self::Inner(value)
    }
}
