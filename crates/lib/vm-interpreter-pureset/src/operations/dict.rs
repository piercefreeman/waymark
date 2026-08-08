/// View the value as a dictionary key.
pub trait AsDictKey<Value> {
    /// The implementation-specific error returned when the value cannot
    /// be used as a dictionary key.
    type Error: core::fmt::Debug;

    /// View the value as a dictionary key.
    fn as_dict_key(value: &Value) -> Result<&str, Self::Error>;
}

/// An error from [`MakeDict::make_dict`].
#[derive(Debug, thiserror::Error)]
pub enum MakeDictError {
    /// The value type does not support dictionary construction.
    #[error("constructing dict values is not supported")]
    NotDictable,

    /// The resulting dictionary could not be represented by the value type.
    #[error("dict result is out of bounds")]
    ResultOutOfBounds,
}

/// Build a dictionary value from a sequence of key-value pairs.
pub trait MakeDict<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
    /// Construct a dictionary value preserving entry order.
    fn make_dict<I>(entries: I) -> Result<Value, MakeDictError>
    where
        I: IntoIterator<Item = (String, Value::RootValue)>;
}

/// The error [`AsDictKey`] returns for `Value`.
pub type AsDictKeyErrorFor<Operations, Value> = <Operations as AsDictKey<Value>>::Error;
