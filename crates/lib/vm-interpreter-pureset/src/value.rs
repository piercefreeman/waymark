//! Value requirements.

/// An error from [`Value::add`].
#[derive(Debug, thiserror::Error)]
pub enum AddError {
    /// At least one operand did not support addition.
    #[error("adding non-addable values")]
    NotAddable,

    /// The addition result could not be represented by the value type.
    #[error("addition result is out of bounds")]
    ResultOutOfBounds,
}

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

/// Add two values together and obtain a sum of them.
pub trait Add: Sized {
    /// Add two resolved values and return the resulting value.
    fn add(a: &Self, b: &Self) -> Result<Self, AddError>;
}

/// Build a list value from a sequence of resolved items.
pub trait MakeList: Sized {
    /// Construct a list value preserving input order.
    fn make_list<I>(items: I) -> Result<Self, MakeListError>
    where
        I: IntoIterator<Item = Self>;
}

/// A unifying trait for all value requirements.
pub trait Value: Add + MakeList {}

impl<T> Value for T where T: Add + MakeList {}
