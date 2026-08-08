/// Resolve attribute access from a value and attribute name.
pub trait DotOp<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
    /// The implementation-specific error returned when the attribute
    /// access fails.
    type Error: core::fmt::Debug;

    /// Read the named attribute from the object.
    fn dot(object: &Value, attribute: &str) -> Result<Value::RootValue, Self::Error>;
}

/// The error [`DotOp`] returns for `Value`.
pub type DotOpErrorFor<Operations, Value> = <Operations as DotOp<Value>>::Error;
