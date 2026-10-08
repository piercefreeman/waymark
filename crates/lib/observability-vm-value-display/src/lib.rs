//! What observability captures of a run for display: the webapp and the
//! other API consumers show these, not the language's own types.
//!
//! Observability does not know what a raised exception carries - that is
//! the language's type, whatever shape it has, whether it surfaces as a
//! promise rejection or as the workflow's unhandled exception. This trait
//! is what observability asks of one, implemented next to the language's
//! type.

#![warn(missing_docs)]

/// A raised exception, reduced to what identifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RaisedExceptionSummary {
    /// The exception's type, as the workflow's language spells it.
    pub exception_type: String,
}

/// Capture a raised exception for observability.
pub trait CaptureRaisedException {
    /// What observability records of this raised exception.
    fn capture_raised_exception(&self) -> RaisedExceptionSummary;
}
