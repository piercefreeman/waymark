//! Common `fn main` things that are fit for any `fn main` in the project.
//!
//! This crate is not supposed to include any "business-logic"-specific things,
//! like bringup logic or executable-specific initialization.
//! Only the common things that would be used in an "arbitrarty" executable
//! are allowed.

#![warn(missing_docs)]

/// Error returned when tracing initialization fails.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InitTracingError(pub Box<dyn std::error::Error + Send + Sync + 'static>);

/// Error returned the initialization fails.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// The `tracing` init failed.
    #[error("tracing: {0}")]
    Tracing(InitTracingError),
}

/// Initializes the global tracing subscriber for the process.
pub fn init_tracing() -> Result<(), InitTracingError> {
    tracing_subscriber::fmt::try_init().map_err(InitTracingError)
}

/// Perform common global initialization.
pub fn init() -> Result<(), InitError> {
    init_tracing().map_err(InitError::Tracing)?;
    Ok(())
}
