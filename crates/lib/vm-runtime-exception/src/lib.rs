//! The core types for supporting exceptions at VM runtime.

#![warn(missing_docs)]

/// The exception type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exception<Details> {
    /// The exception's type identifier.
    pub type_id: String,

    /// The exception's details payload.
    pub details: Details,
}

/// Error returned by [`AsException::as_exception`].
#[derive(Debug, thiserror::Error)]
#[error("the value is not an exception")]
pub struct NotAnExceptionError;

/// Error returned by [`AsException::into_exception`].
#[derive(Debug, thiserror::Error)]
#[error("the value is not an exception")]
pub struct NotAnOwnedExceptionError<Value> {
    /// The consumed value that was not an exception.
    pub value: Value,
}

impl<Value> From<NotAnOwnedExceptionError<Value>> for NotAnExceptionError {
    fn from(_value: NotAnOwnedExceptionError<Value>) -> Self {
        Self
    }
}

/// Borrows the value as an exception.
///
/// If the value is not an exception, returns an error.
pub trait AsException: waymark_vm_runtime_value::RootValueAccess {
    /// Returns this value as a runtime exception ref.
    fn as_exception(&self) -> Result<&Exception<Self::RootValue>, NotAnExceptionError>;
}

/// Consumes and returns the value as an exception.
///
/// If the value is not itself an exception, returns an error that provides
/// the original value.
pub trait IntoException: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Return this value as an owned a runtime exception.
    fn into_exception(self) -> Result<Exception<Self::RootValue>, NotAnOwnedExceptionError<Self>>;
}

/// Constructs a value from a runtime exception.
pub trait FromException: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Wrap the provided exception as `Self`.
    fn from_exception(exception: Exception<Self::RootValue>) -> Self;
}
