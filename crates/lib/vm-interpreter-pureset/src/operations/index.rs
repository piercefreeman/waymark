/// Resolve indexed access from a value and index operand.
pub trait IndexOp<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
    /// The implementation-specific error returned when the indexed
    /// access fails.
    type Error: core::fmt::Debug;

    /// Index into the object using the index value.
    fn index(object: &Value, index: &Value) -> Result<Value::RootValue, Self::Error>;
}

/// The error [`IndexOp`] returns for `Value`.
pub type IndexOpErrorFor<Operations, Value> = <Operations as IndexOp<Value>>::Error;
