//! Values coerced to their `Error` form: seen as one by reference, made
//! one by value.
//!
//! This is the adapter interface for non-[`std::error::Error`] report
//! types (like eyre's and anyhow's). A report unifies errors itself, so
//! it cannot implement the `Error` trait, yet the places that take any
//! error (an error's `source`, a supervisor's task error) still need one
//! from it. The traits here are what such a place asks for instead of the
//! `Error` trait, and an error proper satisfies them by being itself.

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
///
/// The stronger of the two by-reference traits: every `AsError` is an
/// [`AsDynError`] through the blanket impl, its `Error` form erased to
/// `dyn Error`. A value that can name its `Error` form implements this
/// one and gets the other for free.
///
/// One should always prefer implementing `AsError` over [`AsDynError`]
/// when the value can name its `Error` form, because implementing
/// `AsError` automatically provides one with an implementation of
/// [`AsDynError`] thanks to the blanket implementation in this crate.
pub trait AsError {
    /// The `Error` form.
    type Error: std::error::Error + Send + Sync + 'static;

    /// This value seen as its `Error` form.
    fn as_error(&self) -> &Self::Error;
}

/// A value seen as some error by reference, its concrete type erased.
///
/// The weaker of the two by-reference traits: every [`AsError`] is one
/// through the blanket impl, and a value that cannot name one `Error`
/// form, such as a report, implements this one directly and only.
///
/// Prefer using `AsDynError` over [`AsError`] when specifying trait
/// bounds on a generic function, unless the concrete `Error` type is
/// needed. This way, values that only implement `AsDynError`, such as a
/// report, can be used as arguments as well.
pub trait AsDynError {
    /// This value seen as an error.
    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static);
}

impl<T> AsDynError for T
where
    T: AsError,
{
    fn as_dyn_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self.as_error()
    }
}
