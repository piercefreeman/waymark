//! Value requirements.

/// Error returned by [`AsException::as_exception`].
#[derive(Debug, thiserror::Error)]
pub enum AsExceptionError {
    /// The value is not an exception.
    #[error("the value is not an exception")]
    NotAnException,
}

/// Error returned by [`AsExceptionTypeId::as_exception_type_id`].
#[derive(Debug, thiserror::Error)]
pub enum AsExceptionTypeIdError {
    /// The value is not an exception type id.
    #[error("the value is not an exception type id")]
    NotAnExceptionTypeId,
}

/// Borrows the value as an exception.
pub trait AsException: waymark_vm_runtime_value::RootValueAccess {
    /// Returns this value as a runtime exception.
    fn as_exception(
        &self,
    ) -> Result<&waymark_vm_runtime_exception::Exception<Self::RootValue>, AsExceptionError>;
}

/// Borrows the value as an exception type identifier.
pub trait AsExceptionTypeId {
    /// Returns this value as an exception type identifier string.
    fn as_exception_type_id(&self) -> Result<&str, AsExceptionTypeIdError>;
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

/// A unifying trait for all value requirements.
pub trait Value:
    waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
    + AsException
    + AsExceptionTypeId
    + FromIsException
    + CaptureExceptionDetails
{
}

impl<T> Value for T where
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
        + AsException
        + AsExceptionTypeId
        + FromIsException
        + CaptureExceptionDetails
{
}
