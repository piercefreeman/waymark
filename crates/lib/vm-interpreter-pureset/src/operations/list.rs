/// An error from [`MakeList::make_list`].
#[derive(Debug, thiserror::Error)]
pub enum MakeListError {
    /// The value type does not support list construction.
    #[error("constructing list values is not supported")]
    NotListable,

    /// The resulting list could not be represented by the value type.
    #[error("list result is out of bounds")]
    ResultOutOfBounds,
}

/// Build a list value from a sequence of items.
pub trait MakeList<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
    /// Construct a list value preserving input order.
    fn make_list<I>(items: I) -> Result<Value, MakeListError>
    where
        I: IntoIterator<Item = Value::RootValue>;
}

/// An error from [`ListAppend::list_append`].
#[derive(Debug, thiserror::Error)]
pub enum ListAppendError {
    /// The receiver is not a list value.
    #[error("appending requires a list receiver")]
    NotListable,

    /// The grown list could not be represented by the value type.
    #[error("appended list is out of bounds")]
    ResultOutOfBounds,
}

/// Append one item onto a list value, producing a new list.
pub trait ListAppend<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
    /// Returns `list` with `item` appended at the end.
    fn list_append(list: &Value, item: Value::RootValue) -> Result<Value, ListAppendError>;
}
