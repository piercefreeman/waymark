//! Values that have an `Error` form.

#![warn(missing_docs)]

#[cfg(feature = "eyre")]
mod eyre;

/// A value with an `Error` form: itself when it is one, a wrapper when it
/// is not, such as a report. Seen as one by reference, made one by value.
pub trait IntoError {
    /// The `Error` form.
    type Error: std::error::Error + Send + Sync + 'static;

    /// This value seen as the error it is.
    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static);

    /// This value in its `Error` form.
    fn into_error(self) -> Self::Error;
}
