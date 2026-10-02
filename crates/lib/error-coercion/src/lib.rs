//! Values coerced to their `Error` form: seen as one by reference, made
//! one by value.
//!
//! This is the adapter interface for non-[`std::error::Error`] report
//! types (like eyre's and anyhow's). A report unifies errors itself, so
//! it cannot implement the [`std::error::Error`] trait, yet the places
//! that take any error (an error's [`std::error::Error::source`], for
//! one) need the report to be an [`std::error::Error`].
//!
//! The traits here are for the places that abstract over such reports
//! and want to coerce them into [`std::error::Error`]-implementing
//! values.

#![warn(missing_docs)]

#[cfg(feature = "eyre")]
mod eyre;

/// A value with an `Error` form: itself when it is one, a wrapper when it
/// is not, such as a report.
pub trait IntoError {
    /// The `Error` form.
    type Error: std::error::Error + Send + Sync + 'static;

    /// This value in its `Error` form.
    fn into_error(self) -> Self::Error;
}

/// A value seen as its `Error` form by reference: the error it is, or
/// the error it holds.
pub trait AsError {
    /// The `Error` form.
    type Error: std::error::Error + Send + Sync + 'static;

    /// This value seen as its `Error` form.
    fn as_error(&self) -> &Self::Error;
}

/// A value seen as some error by reference, its concrete type erased.
///
/// Independent of [`AsError`]: the error seen through this trait is not
/// necessarily an [`AsError::Error`], and a type implements each of the
/// two on its own.
pub trait AsDynError {
    /// This value seen as an error.
    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static);
}
