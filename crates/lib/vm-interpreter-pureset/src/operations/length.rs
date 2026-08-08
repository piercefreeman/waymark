/// Compute and materialize container lengths.
pub trait Length<Value> {
    /// The type for internal representation of a value length.
    ///
    /// Typically [`usize`].
    type Length;

    /// The implementation-specific error returned when the value has no
    /// length.
    type Error: core::fmt::Debug;

    /// The implementation-specific error returned when a length cannot
    /// be materialized back into the value type.
    type FromLengthError: core::fmt::Debug;

    /// Determine the length of the value.
    fn length(value: &Value) -> Result<Self::Length, Self::Error>;

    /// Materialize a length result back into the VM value type.
    fn from_length(length: Self::Length) -> Result<Value, Self::FromLengthError>;
}

/// The error [`Length::length`] returns for `Value`.
pub type LengthErrorFor<Operations, Value> = <Operations as Length<Value>>::Error;

/// The error [`Length::from_length`] returns for `Value`.
pub type FromLengthErrorFor<Operations, Value> = <Operations as Length<Value>>::FromLengthError;
