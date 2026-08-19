//! The wire format boundary: converting data to and from bytes.
//!
//! "Wire" is any byte boundary, not only a network one — a transport
//! putting a payload on a socket, a snapshot written to a database
//! column, an outcome stored in a row.  Whoever owns the boundary holds
//! the codec: a transport owns the format it speaks, and the
//! persistence layer owns the format of what it stores.  A boundary
//! that never leaves the process — an in-process call — holds a codec
//! that converts nothing and pays nothing for it.
//!
//! A codec is one value, carrying whatever configuration the format
//! takes, and it speaks every kind of data that format can carry: the
//! data is a parameter of these traits, not a property of the codec.
//! The data is not only VM values either — anything that crosses a byte
//! boundary converts through these traits, a value or a snapshot alike.
//!
//! Failure, in contrast, *is* a property of the codec: a format fails
//! the same way whatever it was asked to carry.  Each direction
//! therefore states its error once, on a trait of its own that the
//! data-carrying trait requires — so a caller reads one error type from
//! a codec, not one per datum.
//!
//! The two directions are separate traits: a codec may be able to write
//! a format it never reads, or the reverse.

#![warn(missing_docs)]

/// The failure of writing a wire format.
pub trait ToWireFormatError {
    /// The error returned when writing fails.
    type Error: core::fmt::Debug;
}

/// Writes `Data` in a wire format.
///
/// The data is deliberately unbounded here: what the wire format
/// requires of it is the implementation's business, stated on the impl,
/// never demanded at this surface.
pub trait ToWireFormat<Data>: ToWireFormatError {
    /// Write `data` into `buffer` in this codec's wire format.
    ///
    /// The bytes are *appended*: `buffer` may be shared and already
    /// hold unrelated data, and whatever it holds on entry is left
    /// untouched.  On error the appended region is unspecified — a
    /// caller reusing the buffer truncates it back to the length it
    /// observed before the call.
    fn write_wire_format(&self, data: &Data, buffer: &mut Vec<u8>) -> Result<(), Self::Error>;
}

/// The failure of reading a wire format.
pub trait FromWireFormatError {
    /// The error returned when reading fails.
    type Error: core::fmt::Debug;
}

/// Reads `Data` from a wire format, out of bytes living for `'d`.
///
/// The input lifetime is a parameter of the trait so that `Data` may
/// borrow the bytes it was read from: a codec reading borrowed data
/// implements this once per input lifetime, exactly as
/// [`serde::Deserialize`] does.  Data that never borrows is read for
/// every lifetime, which earns [`FromWireFormatOwned`] for free.
///
/// The data is deliberately unbounded here: what the wire format
/// requires of it is the implementation's business, stated on the impl,
/// never demanded at this surface.
///
/// [`serde::Deserialize`]: https://docs.rs/serde/latest/serde/trait.Deserialize.html
pub trait FromWireFormat<'d, Data>: FromWireFormatError {
    /// Read data from `bytes` in this codec's wire format.
    fn read_wire_format(&self, bytes: &'d [u8]) -> Result<Data, Self::Error>;
}

/// The failure of reading a wire format into data that does not borrow
/// it.
pub trait FromWireFormatOwnedError {
    /// The error returned when reading fails.
    type Error: core::fmt::Debug;
}

impl<Codec> FromWireFormatOwnedError for Codec
where
    Codec: FromWireFormatError,
{
    type Error = <Codec as FromWireFormatError>::Error;
}

/// Reads `Data` that does not borrow the bytes it came from.
///
/// The counterpart of [`serde::de::DeserializeOwned`]: implemented for
/// free wherever the data is read the same way whatever the input
/// lifetime, and useful wherever a caller would otherwise have to carry
/// a higher-ranked bound to say so.
///
/// [`serde::de::DeserializeOwned`]: https://docs.rs/serde/latest/serde/de/trait.DeserializeOwned.html
pub trait FromWireFormatOwned<Data>: FromWireFormatOwnedError {
    /// Read data from `bytes` in this codec's wire format.
    fn read_wire_format_owned(&self, bytes: &[u8]) -> Result<Data, Self::Error>;
}

impl<Codec, Data> FromWireFormatOwned<Data> for Codec
where
    Codec: for<'d> FromWireFormat<'d, Data>,
{
    fn read_wire_format_owned(&self, bytes: &[u8]) -> Result<Data, Self::Error> {
        self.read_wire_format(bytes)
    }
}
