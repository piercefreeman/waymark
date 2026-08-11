//! Wire format over a serde codec.
//!
//! Adapts the [`waymark_vm_codec_core`] serializer/deserializer
//! providers — the existing serde-shaped codecs, MessagePack among
//! them — to the wire format traits, so a serde-serializable datum
//! crosses a byte boundary without a bespoke encoding of its own.

#![warn(missing_docs)]

/// Wire format codec over a serde codec.
///
/// Speaks every datum the wrapped codec's serde implementations can
/// carry.
#[derive(Debug, Clone, Copy, Default)]
pub struct SerdeCodec<Codec>(
    /// The serde codec supplying the serializer and deserializer.
    pub Codec,
);

impl<Codec> waymark_vm_wire_format::ToWireFormatError for SerdeCodec<Codec>
where
    Codec: waymark_vm_codec_core::SerializerProvider,
{
    type Error = Codec::Error;
}

/// Writes the datum through the codec's serializer.
///
/// Appending is the serializer provider's behaviour: it writes into the
/// buffer it is handed, leaving what the buffer already holds in place.
impl<Codec, Data> waymark_vm_wire_format::ToWireFormat<Data> for SerdeCodec<Codec>
where
    Codec: waymark_vm_codec_core::SerializerProvider,
    Data: serde::Serialize,
{
    fn write_wire_format(&self, data: &Data, buffer: &mut Vec<u8>) -> Result<(), Codec::Error> {
        self.0
            .with_serializer(buffer, |serializer| data.serialize(serializer))?;

        Ok(())
    }
}

impl<Codec> waymark_vm_wire_format::FromWireFormatError for SerdeCodec<Codec>
where
    Codec: waymark_vm_codec_core::DeserializerProvider,
{
    type Error = Codec::Error;
}

/// Reads the datum in its owned form.
///
/// Owned data is the whole of what this adapter can produce: the
/// deserializer provider hands out a deserializer valid for any
/// lifetime, so nothing read through it can borrow the input bytes —
/// hence the implementation for every `'d` rather than for data that
/// borrows one.  Reading borrowed data through serde means building the
/// deserializer over the input directly, without this provider in the
/// way.
impl<'d, Codec, Data> waymark_vm_wire_format::FromWireFormat<'d, Data> for SerdeCodec<Codec>
where
    Codec: waymark_vm_codec_core::DeserializerProvider,
    Data: serde::de::DeserializeOwned,
{
    fn read_wire_format(&self, bytes: &'d [u8]) -> Result<Data, Codec::Error> {
        self.0
            .with_deserializer(bytes, |deserializer| Data::deserialize(deserializer))
    }
}

#[cfg(test)]
mod tests {
    use waymark_vm_codec_rmp::RmpCodec;
    use waymark_vm_wire_format::{FromWireFormatOwned as _, ToWireFormat as _};

    use super::*;

    #[test]
    fn round_trip() {
        let codec = SerdeCodec(RmpCodec);

        let mut buffer = Vec::new();
        codec.write_wire_format(&42_u32, &mut buffer).unwrap();

        let read: u32 = codec.read_wire_format_owned(&buffer).unwrap();

        assert_eq!(read, 42);
    }

    #[test]
    fn appends_to_a_shared_buffer() {
        let codec = SerdeCodec(RmpCodec);

        let mut buffer = b"already here".to_vec();
        let offset = buffer.len();
        codec.write_wire_format(&42_u32, &mut buffer).unwrap();

        assert_eq!(&buffer[..offset], b"already here");

        let read: u32 = codec.read_wire_format_owned(&buffer[offset..]).unwrap();

        assert_eq!(read, 42);
    }

    #[test]
    fn speaks_more_than_one_datum() {
        let codec = SerdeCodec(RmpCodec);

        let mut buffer = Vec::new();
        codec.write_wire_format(&42_u32, &mut buffer).unwrap();
        let offset = buffer.len();
        codec
            .write_wire_format(&"hello".to_owned(), &mut buffer)
            .unwrap();

        let number: u32 = codec.read_wire_format_owned(&buffer[..offset]).unwrap();
        let text: String = codec.read_wire_format_owned(&buffer[offset..]).unwrap();

        assert_eq!((number, text.as_str()), (42, "hello"));
    }
}
