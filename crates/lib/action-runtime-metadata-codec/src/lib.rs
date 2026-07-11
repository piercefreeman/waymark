//! Streaming encoder / decoder traits for metadata identifiers.
//!
//! These traits decouple id serialisation from any particular byte-length
//! or binary layout.  [`Encode`] appends bytes to a writer;
//! [`Decode`] consumes bytes from a slice, advancing the cursor.
//! Neither trait assumes a fixed size — the wire format is owned entirely
//! by the implementing type.

#![warn(missing_docs)]

/// A value that can serialise itself into a byte stream.
pub trait Encode {
    /// Append the encoded form of this value to `writer`.
    fn encode(&self, writer: &mut Vec<u8>);
}

/// A value that can be deserialised from a byte stream.
pub trait Decode: Sized {
    /// The error returned when the input bytes do not represent a valid value.
    type Error: core::fmt::Debug + core::fmt::Display;

    /// Read and decode a value from the front of `input`, advancing
    /// the slice past the consumed bytes.
    fn decode(input: &mut &[u8]) -> Result<Self, Self::Error>;
}
