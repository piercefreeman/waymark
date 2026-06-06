//! Codec abstraction for runtime snapshot serialization/deserialization.

#![warn(missing_docs)]

mod arc;
mod tuple;

/// Serializes values into a byte buffer.
pub trait SnapshotSerializer {
    /// The serializer type, borrowing the output buffer.
    type Serializer<'buf>: serde::Serializer<Ok = (), Error = Self::Error>
    where
        Self: 'buf;

    /// Error returned when serialization fails.
    type Error: std::fmt::Debug;

    /// Call `f` with a serializer that writes into `buffer`.
    ///
    /// The implementor creates a serializer, hands `&mut` of it to `f`,
    /// and the result is written into `buffer`.  This allows the caller
    /// to use `value.snapshot(serializer)`-style APIs.
    fn with_serializer<F, T>(&self, buffer: &mut Vec<u8>, f: F) -> Result<T, Self::Error>
    where
        for<'buf> F: FnOnce(Self::Serializer<'buf>) -> Result<T, Self::Error>;
}

/// Deserializes values from a byte slice.
pub trait SnapshotDeserializer {
    /// The deserializer type, borrowing the input data.
    type Deserializer<'de>: serde::Deserializer<'de, Error = Self::Error>
    where
        Self: 'de;

    /// Error returned when deserialization fails.
    type Error: std::fmt::Debug;

    /// Call `f` with a deserializer that reads from `data`.
    fn with_deserializer<F, T>(&self, data: &[u8], f: F) -> Result<T, Self::Error>
    where
        for<'de> F: FnOnce(Self::Deserializer<'de>) -> Result<T, Self::Error>;
}
