//! Protobuf wire format with conversion indirection.
//!
//! The codec's generics are the wire message — the [`prost::Message`]
//! that actually becomes the bytes — the converter that realizes the
//! indirection, and the conversion errors of the two directions.  The
//! data the codec is asked to carry converts into the message on the
//! way out and back from it on the way in: the message is the
//! indirection between the caller's data and the wire, and the proto
//! tree stays the codec's private business.
//!
//! For the Python flavor's values the wiring pins
//! `ProtoCodec<python_value::Value, Converter, …>` with the
//! `vm-value-python-convert-proto` converter: it writes a `ReadyValue`
//! as the encoded `python_value::Value` message — exactly the bytes the
//! framing-level `WorkflowArgument.value` carries today.
//!
//! A boundary that only writes or only reads holds the matching half:
//! [`ProtoEncoder`] and [`ProtoDecoder`] each speak one direction, the
//! dual [`ProtoCodec`] hands out either half via
//! [`encoder`](ProtoCodec::encoder) / [`decoder`](ProtoCodec::decoder),
//! and [`combine`] puts an encoder and a decoder back together.
//!
//! # Disposable by construction
//!
//! This codec is frozen at today's value roster; its whole job is to be
//! what the CBOR codec replaces.  Do NOT extend it.

#![warn(missing_docs)]

use std::marker::PhantomData;

/// The error returned when reading the proto wire format fails.
///
/// Reading fails two ways: the bytes do not decode as the wire message,
/// or the decoded message does not convert into the requested data.
#[derive(Debug, thiserror::Error)]
pub enum ReadError<ConversionError> {
    /// The bytes did not decode as the wire message.
    #[error("decoding the wire message")]
    Decode(#[source] prost::DecodeError),

    /// The decoded message did not convert into the requested data.
    #[error("converting the decoded wire message")]
    Convert(#[source] ConversionError),
}

/// Protobuf codec, generic over the wire message it speaks, the
/// converter that realizes the indirection, and the conversion errors
/// of the two directions.
///
/// `Message` is the indirection: the data converts to and from it via
/// `Converter`, and the message converts to and from the bytes.
pub struct ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError> {
    /// Phantom data for the type parameters.
    pub phantom_data: PhantomData<(
        Message,
        Converter,
        WriteConversionError,
        ReadConversionError,
    )>,
}

// Manual impls: the codec holds no data, so none of these depend on the
// type parameters — a derive would bound them anyway.
impl<Message, Converter, WriteConversionError, ReadConversionError> core::fmt::Debug
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("ProtoCodec").finish()
    }
}

impl<Message, Converter, WriteConversionError, ReadConversionError> Clone
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Message, Converter, WriteConversionError, ReadConversionError> Copy
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
{
}

impl<Message, Converter, WriteConversionError, ReadConversionError> Default
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
{
    fn default() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<Message, Converter, WriteConversionError, ReadConversionError>
    waymark_vm_wire_format::ToWireFormatError
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
where
    WriteConversionError: core::fmt::Debug,
{
    // Writing fails only by failing to convert: the buffer written into
    // is a `Vec`, which never lacks capacity, so the encoding itself
    // cannot fail.
    type Error = WriteConversionError;
}

impl<Data, Message, Converter, WriteConversionError, ReadConversionError>
    waymark_vm_wire_format::ToWireFormat<Data>
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
where
    Message: prost::Message,
    WriteConversionError: core::fmt::Debug,
    for<'data> Converter:
        waymark_convert_core::TryConvert<&'data Data, Message, Error = WriteConversionError>,
{
    fn write_wire_format(
        &self,
        data: &Data,
        buffer: &mut Vec<u8>,
    ) -> Result<(), WriteConversionError> {
        let message: Message = Converter::try_convert(data)?;
        message
            .encode(buffer)
            .expect("a Vec buffer never lacks capacity");
        Ok(())
    }
}

impl<Message, Converter, WriteConversionError, ReadConversionError>
    waymark_vm_wire_format::FromWireFormatError
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
where
    ReadConversionError: core::fmt::Debug,
{
    type Error = ReadError<ReadConversionError>;
}

impl<'d, Data, Message, Converter, WriteConversionError, ReadConversionError>
    waymark_vm_wire_format::FromWireFormat<'d, Data>
    for ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
where
    Message: prost::Message + Default,
    ReadConversionError: core::fmt::Debug,
    for<'message> Converter:
        waymark_convert_core::TryConvert<&'message Message, Data, Error = ReadConversionError>,
{
    fn read_wire_format(&self, bytes: &'d [u8]) -> Result<Data, ReadError<ReadConversionError>> {
        let message = Message::decode(bytes).map_err(ReadError::Decode)?;
        Converter::try_convert(&message).map_err(ReadError::Convert)
    }
}

/// Protobuf encoder: the writing half of [`ProtoCodec`].
///
/// Holds only the write direction, for a boundary that never reads.
pub struct ProtoEncoder<Message, Converter, ConversionError> {
    /// Phantom data for the type parameters.
    pub phantom_data: PhantomData<(Message, Converter, ConversionError)>,
}

// Manual impls: the encoder holds no data, so none of these depend on
// the type parameters — a derive would bound them anyway.
impl<Message, Converter, ConversionError> core::fmt::Debug
    for ProtoEncoder<Message, Converter, ConversionError>
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("ProtoEncoder").finish()
    }
}

impl<Message, Converter, ConversionError> Clone
    for ProtoEncoder<Message, Converter, ConversionError>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Message, Converter, ConversionError> Copy
    for ProtoEncoder<Message, Converter, ConversionError>
{
}

impl<Message, Converter, ConversionError> Default
    for ProtoEncoder<Message, Converter, ConversionError>
{
    fn default() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<Message, Converter, ConversionError> waymark_vm_wire_format::ToWireFormatError
    for ProtoEncoder<Message, Converter, ConversionError>
where
    ConversionError: core::fmt::Debug,
{
    // Writing fails only by failing to convert: the buffer written into
    // is a `Vec`, which never lacks capacity, so the encoding itself
    // cannot fail.
    type Error = ConversionError;
}

impl<Data, Message, Converter, ConversionError> waymark_vm_wire_format::ToWireFormat<Data>
    for ProtoEncoder<Message, Converter, ConversionError>
where
    Message: prost::Message,
    ConversionError: core::fmt::Debug,
    for<'data> Converter:
        waymark_convert_core::TryConvert<&'data Data, Message, Error = ConversionError>,
{
    fn write_wire_format(&self, data: &Data, buffer: &mut Vec<u8>) -> Result<(), ConversionError> {
        let message: Message = Converter::try_convert(data)?;
        message
            .encode(buffer)
            .expect("a Vec buffer never lacks capacity");
        Ok(())
    }
}

/// Protobuf decoder: the reading half of [`ProtoCodec`].
///
/// Holds only the read direction, for a boundary that never writes.
pub struct ProtoDecoder<Message, Converter, ConversionError> {
    /// Phantom data for the type parameters.
    pub phantom_data: PhantomData<(Message, Converter, ConversionError)>,
}

// Manual impls: the decoder holds no data, so none of these depend on
// the type parameters — a derive would bound them anyway.
impl<Message, Converter, ConversionError> core::fmt::Debug
    for ProtoDecoder<Message, Converter, ConversionError>
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("ProtoDecoder").finish()
    }
}

impl<Message, Converter, ConversionError> Clone
    for ProtoDecoder<Message, Converter, ConversionError>
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<Message, Converter, ConversionError> Copy
    for ProtoDecoder<Message, Converter, ConversionError>
{
}

impl<Message, Converter, ConversionError> Default
    for ProtoDecoder<Message, Converter, ConversionError>
{
    fn default() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<Message, Converter, ConversionError> waymark_vm_wire_format::FromWireFormatError
    for ProtoDecoder<Message, Converter, ConversionError>
where
    ConversionError: core::fmt::Debug,
{
    type Error = ReadError<ConversionError>;
}

impl<'d, Data, Message, Converter, ConversionError> waymark_vm_wire_format::FromWireFormat<'d, Data>
    for ProtoDecoder<Message, Converter, ConversionError>
where
    Message: prost::Message + Default,
    ConversionError: core::fmt::Debug,
    for<'message> Converter:
        waymark_convert_core::TryConvert<&'message Message, Data, Error = ConversionError>,
{
    fn read_wire_format(&self, bytes: &'d [u8]) -> Result<Data, ReadError<ConversionError>> {
        let message = Message::decode(bytes).map_err(ReadError::Decode)?;
        Converter::try_convert(&message).map_err(ReadError::Convert)
    }
}

impl<Message, Converter, WriteConversionError, ReadConversionError>
    ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError>
{
    /// The writing half of this codec.
    pub fn encoder(&self) -> ProtoEncoder<Message, Converter, WriteConversionError> {
        ProtoEncoder::default()
    }

    /// The reading half of this codec.
    pub fn decoder(&self) -> ProtoDecoder<Message, Converter, ReadConversionError> {
        ProtoDecoder::default()
    }
}

/// Combine an encoder and a decoder into the dual codec.
///
/// The two halves must agree on the wire message and the converter —
/// the signature enforces it.
pub fn combine<Message, Converter, WriteConversionError, ReadConversionError>(
    _encoder: ProtoEncoder<Message, Converter, WriteConversionError>,
    _decoder: ProtoDecoder<Message, Converter, ReadConversionError>,
) -> ProtoCodec<Message, Converter, WriteConversionError, ReadConversionError> {
    ProtoCodec::default()
}

#[cfg(test)]
mod tests {
    use waymark_convert_core::TryConvert as _;
    use waymark_vm_value_python::ReadyValue;
    use waymark_vm_wire_format::{FromWireFormatOwned as _, ToWireFormat as _};

    use super::*;

    /// The codec pinned to the Python flavor's value message and
    /// converter, its errors named by projection.
    type ValueCodec = ProtoCodec<
        waymark_proto::python_value::Value,
        waymark_vm_value_python_convert_proto::Converter,
        waymark_convert_core::ConvertErrorFor<
            waymark_vm_value_python_convert_proto::Converter,
            &'static ReadyValue,
            waymark_proto::python_value::Value,
        >,
        core::convert::Infallible,
    >;

    fn ready(value: ReadyValue) -> waymark_vm_value_python::Value {
        waymark_vm_value_python::Value::Ready(value)
    }

    fn round_trip(value: &ReadyValue) -> ReadyValue {
        let mut buffer = Vec::new();
        ValueCodec::default()
            .write_wire_format(value, &mut buffer)
            .expect("no pending promise in the value");
        ValueCodec::default()
            .read_wire_format_owned(&buffer)
            .expect("the codec reads what it wrote")
    }

    #[test]
    fn values_round_trip() {
        let value = ReadyValue::Dict(indexmap::IndexMap::from([
            ("int".to_owned(), ready(ReadyValue::Int(42))),
            (
                "list".to_owned(),
                ready(ReadyValue::List(vec![ready(ReadyValue::String(
                    "hello".to_owned(),
                ))])),
            ),
        ]));

        assert_eq!(round_trip(&value), value);
    }

    #[test]
    fn dict_keeps_its_insertion_order() {
        // Insertion order is part of the value; the format carries it.
        let keys = ["zebra", "apple", "mango"];
        let value = ReadyValue::Dict(
            keys.iter()
                .enumerate()
                .map(|(index, key)| ((*key).to_owned(), ready(ReadyValue::Int(index as i64))))
                .collect(),
        );

        let ReadyValue::Dict(entries) = round_trip(&value) else {
            panic!("a dict round trips as a dict");
        };
        let round_tripped: Vec<_> = entries.keys().map(String::as_str).collect();
        assert_eq!(round_tripped, keys);
    }

    #[test]
    fn writes_exactly_the_bytes_the_framing_carries() {
        // The codec's contract: its bytes ARE the encoded proto value
        // message, byte for byte — what `WorkflowArgument.value` carries.
        let value = ReadyValue::List(vec![ready(ReadyValue::Int(7)), ready(ReadyValue::None)]);

        let mut buffer = Vec::new();
        ValueCodec::default()
            .write_wire_format(&value, &mut buffer)
            .expect("no pending promise in the value");

        let direct: waymark_proto::python_value::Value =
            waymark_vm_value_python_convert_proto::Converter::try_convert(&value)
                .expect("no pending promise in the value");
        assert_eq!(buffer, prost::Message::encode_to_vec(&direct));
    }

    #[test]
    fn appends_to_a_shared_buffer() {
        let mut buffer = b"already here".to_vec();
        let offset = buffer.len();
        ValueCodec::default()
            .write_wire_format(&ReadyValue::Int(42), &mut buffer)
            .expect("no pending promise in the value");

        assert_eq!(&buffer[..offset], b"already here");
        let read: ReadyValue = ValueCodec::default()
            .read_wire_format_owned(&buffer[offset..])
            .expect("the codec reads what it wrote");
        assert_eq!(read, ReadyValue::Int(42));
    }

    #[test]
    fn splits_and_combines() {
        // Each half speaks its one direction, and combining the halves
        // is the dual codec again.
        let codec = ValueCodec::default();
        let encoder = codec.encoder();
        let decoder = codec.decoder();

        let mut buffer = Vec::new();
        encoder
            .write_wire_format(&ReadyValue::Int(9), &mut buffer)
            .expect("no pending promise in the value");
        let read: ReadyValue = decoder
            .read_wire_format_owned(&buffer)
            .expect("the decoder reads what the encoder wrote");
        assert_eq!(read, ReadyValue::Int(9));

        let combined: ValueCodec = combine(encoder, decoder);
        let read: ReadyValue = combined
            .read_wire_format_owned(&buffer)
            .expect("the combined codec reads what the encoder wrote");
        assert_eq!(read, ReadyValue::Int(9));
    }

    #[test]
    fn a_pending_promise_cannot_be_written() {
        let value = ReadyValue::List(vec![waymark_vm_value_python::Value::Pending(
            waymark_vm_runtime_promise_core::PromiseStateId(7),
        )]);

        let mut buffer = Vec::new();
        let written = ValueCodec::default().write_wire_format(&value, &mut buffer);

        let error = written.expect_err("a pending promise has no encoding");
        assert!(format!("{error:?}").contains("PromiseStateId(7)"));
    }
}
