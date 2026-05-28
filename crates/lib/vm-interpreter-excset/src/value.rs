//! Value requirements.
//!
//! Only the borrowed exception shape lives in [`waymark_vm_runtime_exception`].
//! ExcSet-specific queries and conversions stay local to this crate.

/// Error returned by [`AsExceptionTypeId::as_exception_type_id`].
#[derive(Debug, thiserror::Error)]
#[error("the value is not an exception type id")]
pub struct NotAnExceptionTypeIdError;

/// Borrows the value as an exception type identifier.
pub trait AsExceptionTypeId {
    /// Returns this value as an exception type identifier string.
    fn as_exception_type_id(&self) -> Result<&str, NotAnExceptionTypeIdError>;
}

/// Builds a value from the result of an exception type check.
pub trait FromIsException: waymark_vm_runtime_value::RootValueAccess {
    /// Converts the boolean result of an exception check into a root value.
    fn from_is_exception(is_exception: bool) -> Self::RootValue;
}

/// Produces a value from an exception's details payload.
pub trait CaptureExceptionDetails: waymark_vm_runtime_value::RootValueAccess {
    /// Copies or re-materializes an exception's details payload as a root value.
    fn from_exception_details(value: &Self::RootValue) -> Self::RootValue;
}

/// Reports whether a value is currently an exception.
pub trait IsException {
    /// Returns `true` iff this value holds an exception.
    fn is_exception(&self) -> bool;
}

impl<T: waymark_vm_runtime_exception::AsException> IsException for T {
    fn is_exception(&self) -> bool {
        self.as_exception().is_ok()
    }
}

/// A unifying trait for all value requirements.
pub trait Value:
    waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
    + waymark_vm_runtime_exception::AsException
    + AsExceptionTypeId
    + FromIsException
    + CaptureExceptionDetails
{
}

impl<T> Value for T where
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
        + waymark_vm_runtime_exception::AsException
        + AsExceptionTypeId
        + FromIsException
        + CaptureExceptionDetails
{
}
