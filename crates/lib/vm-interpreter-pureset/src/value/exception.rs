/// An error from [`AsExceptionTypeId::as_exception_type_id`].
#[derive(Debug, thiserror::Error)]
pub enum AsExceptionTypeIdError {
    /// The value can't be used as an exception type id.
    #[error("exception type ids of this type are not supported")]
    UnsupportedTypeIdType,
}

/// View the value as an exception type id.
pub trait AsExceptionTypeId {
    /// View the value as an exception type id.
    fn as_exception_type_id(&self) -> Result<&str, AsExceptionTypeIdError>;
}

/// Build an exception value from a type id and a details payload.
pub trait MakeException: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Construct an exception value.
    fn make_exception(type_id: String, details: Self::RootValue) -> Self;
}
