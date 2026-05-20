/// An error from [`DotOp::dot`].
#[derive(Debug, thiserror::Error)]
pub enum DotOperationError {
    /// The value type does not support attribute access for this object.
    #[error("attribute access is not supported for this value")]
    UnsupportedOperation,

    /// The target object does not contain the requested attribute.
    #[error("attribute is missing")]
    MissingAttribute,
}

/// Resolve attribute access from a value and attribute name.
pub trait DotOp: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Read the named attribute from the object.
    fn dot(object: &Self, attribute: &str) -> Result<Self::RootValue, DotOperationError> {
        let _ = (object, attribute);
        Err(DotOperationError::UnsupportedOperation)
    }
}
