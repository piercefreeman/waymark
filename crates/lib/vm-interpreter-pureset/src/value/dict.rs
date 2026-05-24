/// An error from [`AsDictKey::as_dict_key`].
#[derive(Debug, thiserror::Error)]
pub enum AsDictKeyError {
    /// The value can't be used as a dict key.
    #[error("dict keys of this type are not supported")]
    UnsupportedKeyType,
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

/// View the value as a dictionary key.
pub trait AsDictKey {
    /// View the value as a dictionary key.
    fn as_dict_key(&self) -> Result<&str, AsDictKeyError>;
}

/// Build a dictionary value from a sequence of key-value pairs.
pub trait MakeDict: Sized + waymark_vm_runtime_value::RootValueAccess {
    /// Construct a dictionary value preserving entry order.
    fn make_dict<I>(entries: I) -> Result<Self, MakeDictError>
    where
        I: IntoIterator<Item = (String, Self::RootValue)>;
}
