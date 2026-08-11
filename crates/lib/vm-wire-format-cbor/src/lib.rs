//! CBOR wire format, backed by [`minicbor`].
//!
//! One CBOR document per written datum, appended to the caller's
//! buffer.

#![warn(missing_docs)]

/// The error returned when writing CBOR fails.
///
/// The buffer written into is a [`Vec`], which never fails to grow, so
/// every error here originates in the datum's own encoding.
pub type WriteError = minicbor::encode::Error<core::convert::Infallible>;

/// The error returned when reading CBOR fails.
pub type ReadError = minicbor::decode::Error;

/// CBOR codec.
///
/// Speaks every datum minicbor can encode or decode.
#[derive(Debug, Clone, Copy, Default)]
pub struct CborCodec;

impl waymark_vm_wire_format::ToWireFormatError for CborCodec {
    type Error = WriteError;
}

impl<Data> waymark_vm_wire_format::ToWireFormat<Data> for CborCodec
where
    Data: minicbor::Encode<()>,
{
    fn write_wire_format(&self, data: &Data, buffer: &mut Vec<u8>) -> Result<(), WriteError> {
        minicbor::encode(data, buffer)
    }
}

impl waymark_vm_wire_format::FromWireFormatError for CborCodec {
    type Error = ReadError;
}

impl<'d, Data> waymark_vm_wire_format::FromWireFormat<'d, Data> for CborCodec
where
    Data: minicbor::Decode<'d, ()>,
{
    fn read_wire_format(&self, bytes: &'d [u8]) -> Result<Data, ReadError> {
        minicbor::decode(bytes)
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_wire_format::{
        FromWireFormat as _, FromWireFormatOwned as _, ToWireFormat as _,
    };

    use super::*;

    #[test]
    fn round_trip() {
        let mut buffer = Vec::new();
        CborCodec.write_wire_format(&42_u32, &mut buffer).unwrap();

        let read: u32 = CborCodec.read_wire_format_owned(&buffer).unwrap();

        assert_eq!(read, 42);
    }

    #[test]
    fn appends_to_a_shared_buffer() {
        let mut buffer = b"already here".to_vec();
        let offset = buffer.len();
        CborCodec.write_wire_format(&42_u32, &mut buffer).unwrap();

        assert_eq!(&buffer[..offset], b"already here");

        let read: u32 = CborCodec.read_wire_format_owned(&buffer[offset..]).unwrap();

        assert_eq!(read, 42);
    }

    #[test]
    fn speaks_more_than_one_datum() {
        let mut buffer = Vec::new();
        CborCodec.write_wire_format(&42_u32, &mut buffer).unwrap();
        let offset = buffer.len();
        CborCodec
            .write_wire_format(&"hello".to_owned(), &mut buffer)
            .unwrap();

        let number: u32 = CborCodec.read_wire_format_owned(&buffer[..offset]).unwrap();
        let text: String = CborCodec.read_wire_format_owned(&buffer[offset..]).unwrap();

        assert_eq!((number, text.as_str()), (42, "hello"));
    }

    #[test]
    fn reads_data_borrowed_from_the_input() {
        let mut buffer = Vec::new();
        CborCodec
            .write_wire_format(&"hello".to_owned(), &mut buffer)
            .unwrap();

        let read: &str = CborCodec.read_wire_format(&buffer).unwrap();

        assert_eq!(read, "hello");
        assert!(buffer.as_ptr_range().contains(&read.as_ptr()));
    }
}
