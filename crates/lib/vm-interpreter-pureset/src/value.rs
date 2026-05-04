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

/// Add two values together and obtain a sum of them.
pub trait Add: Sized {
    /// Add two resolved values and return the resulting value.
    fn add(a: &Self, b: &Self) -> Result<Self, AddError>;
}

/// A unifying trait for all value requirements.
pub trait Value: Add {}

impl<T> Value for T where T: Add {}
