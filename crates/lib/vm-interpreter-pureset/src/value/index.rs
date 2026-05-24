/// An error from [`IndexOp::index`].
#[derive(Debug, thiserror::Error)]
pub enum IndexOperationError {
    /// The value type does not support indexed access for the operands.
    #[error("indexed access is not supported for these operands")]
    UnsupportedOperation,

    /// The provided index falls outside the bounds of the target object.
    #[error("index is out of bounds")]
    IndexOutOfBounds,

    /// The target dictionary does not contain the requested key.
    #[error("key is missing")]
    MissingKey,
}

/// Resolve indexed access from a value and index operand.
pub trait IndexOp: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Index into the object using the index value.
    fn index(object: &Self, index: &Self) -> Result<Self::RootValue, IndexOperationError> {
        let _ = (object, index);
        Err(IndexOperationError::UnsupportedOperation)
    }
}
