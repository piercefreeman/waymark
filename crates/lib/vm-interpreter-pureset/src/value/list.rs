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
pub trait MakeList: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Construct a list value preserving input order.
    fn make_list<I>(items: I) -> Result<Self, MakeListError>
    where
        I: IntoIterator<Item = Self::RootValue>;
}
