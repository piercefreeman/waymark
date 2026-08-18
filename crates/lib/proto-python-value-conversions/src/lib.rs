//! Conversions for the Python flavor's proto values.
//!
//! Everything here operates on [`waymark_proto::python_value`] — the
//! encoding of a single Python workflow value — and knows nothing about
//! the framing that carries the encoded values.

use prost::Message as _;
use waymark_proto::python_value as proto_value;

/// Build the exception denoted by a type id and a message.
///
/// The details are an ordinary value carrying the exception's
/// particulars — for an exception minted here rather than raised by the
/// language, that is the message alone.
pub fn exception_value(type_id: String, message: String) -> proto_value::ExceptionValue {
    let message = proto_value::Value {
        kind: Some(proto_value::value::Kind::Primitive(
            proto_value::PrimitiveValue {
                kind: Some(proto_value::primitive_value::Kind::StringValue(message)),
            },
        )),
    };
    let details = proto_value::Value {
        kind: Some(proto_value::value::Kind::DictValue(
            proto_value::DictValue {
                entries: vec![proto_value::DictEntry {
                    key: "message".to_owned(),
                    value: Some(message),
                }],
            },
        )),
    };

    proto_value::ExceptionValue {
        type_id,
        details: Some(Box::new(details)),
    }
}

/// The result of an action that returned the given value.
pub fn returned_value(value: proto_value::Value) -> proto_value::ActionOutcome {
    proto_value::ActionOutcome {
        outcome: Some(proto_value::action_outcome::Outcome::Value(value)),
    }
}

/// The result of an action that raised the given exception.
pub fn raised_exception(exception: proto_value::ExceptionValue) -> proto_value::ActionOutcome {
    proto_value::ActionOutcome {
        outcome: Some(proto_value::action_outcome::Outcome::Exception(exception)),
    }
}

/// Encode an [`proto_value::ActionOutcome`] into the bytes an action
/// result carries.
pub fn encode_action_outcome(value: &proto_value::ActionOutcome) -> Vec<u8> {
    value.encode_to_vec()
}

/// Decode the result an action result carries.
pub fn decode_action_outcome(
    bytes: &[u8],
) -> Result<proto_value::ActionOutcome, prost::DecodeError> {
    proto_value::ActionOutcome::decode(bytes)
}

/// Encode a [`proto_value::Value`] into the opaque value
/// bytes carried at the framing level.
pub fn encode_value(value: &proto_value::Value) -> Vec<u8> {
    value.encode_to_vec()
}

/// Decode an encoded [`proto_value::Value`].
pub fn decode_value(bytes: &[u8]) -> Result<proto_value::Value, prost::DecodeError> {
    proto_value::Value::decode(bytes)
}

/// Decode an encoded [`proto_value::ExceptionValue`].
pub fn decode_exception_value(
    bytes: &[u8],
) -> Result<proto_value::ExceptionValue, prost::DecodeError> {
    proto_value::ExceptionValue::decode(bytes)
}
