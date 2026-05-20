/// An error from [`Length::length`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LengthError {
    /// The value type does not support reporting a length for this value.
    #[error("determining length is not supported for this value")]
    UnsupportedValue,
}

/// An error from [`Length::from_length`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FromLengthError {
    /// The resulting length could not be represented by the value type.
    #[error("length result is out of bounds")]
    ResultOutOfBounds,
}

/// Compute and materialize container lengths.
pub trait Length: Sized {
    /// The type for internal representation of a value length.
    ///
    /// Typically [`usize`].
    type Length;

    /// Determine the length of the value.
    fn length(&self) -> Result<Self::Length, LengthError>;

    /// Materialize a length result back into the VM value type.
    fn from_length(length: Self::Length) -> Result<Self, FromLengthError>;
}
