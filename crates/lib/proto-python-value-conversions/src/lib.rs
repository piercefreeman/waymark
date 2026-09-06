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
pub fn exception_value(type_id: String, message: String) -> proto_value::WorkflowExceptionValue {
    let message = proto_value::WorkflowArgumentValue {
        kind: Some(proto_value::workflow_argument_value::Kind::Primitive(
            proto_value::PrimitiveWorkflowArgument {
                kind: Some(proto_value::primitive_workflow_argument::Kind::StringValue(
                    message,
                )),
            },
        )),
    };
    let details = proto_value::WorkflowArgumentValue {
        kind: Some(proto_value::workflow_argument_value::Kind::DictValue(
            proto_value::WorkflowDictArgument {
                entries: vec![proto_value::WorkflowDictEntry {
                    key: "message".to_owned(),
                    value: Some(message),
                }],
            },
        )),
    };

    proto_value::WorkflowExceptionValue {
        type_id,
        details: Some(Box::new(details)),
    }
}

/// The result of an action that returned the given value.
pub fn returned_value(value: proto_value::WorkflowArgumentValue) -> proto_value::ActionResultValue {
    proto_value::ActionResultValue {
        outcome: Some(proto_value::action_result_value::Outcome::Value(value)),
    }
}

/// The result of an action that raised the given exception.
pub fn raised_exception(
    exception: proto_value::WorkflowExceptionValue,
) -> proto_value::ActionResultValue {
    proto_value::ActionResultValue {
        outcome: Some(proto_value::action_result_value::Outcome::Exception(
            exception,
        )),
    }
}

/// Encode an [`proto_value::ActionResultValue`] into the bytes an action
/// result carries.
pub fn encode_action_result_value(value: &proto_value::ActionResultValue) -> Vec<u8> {
    value.encode_to_vec()
}

/// Decode the result an action result carries.
pub fn decode_action_result_value(
    bytes: &[u8],
) -> Result<proto_value::ActionResultValue, prost::DecodeError> {
    proto_value::ActionResultValue::decode(bytes)
}

/// Encode a [`proto_value::WorkflowArgumentValue`] into the opaque value
/// bytes carried at the framing level.
pub fn encode_workflow_argument_value(value: &proto_value::WorkflowArgumentValue) -> Vec<u8> {
    value.encode_to_vec()
}

/// Decode an encoded [`proto_value::WorkflowArgumentValue`].
pub fn decode_workflow_argument_value(
    bytes: &[u8],
) -> Result<proto_value::WorkflowArgumentValue, prost::DecodeError> {
    proto_value::WorkflowArgumentValue::decode(bytes)
}

/// Decode an encoded [`proto_value::WorkflowExceptionValue`].
pub fn decode_workflow_exception_value(
    bytes: &[u8],
) -> Result<proto_value::WorkflowExceptionValue, prost::DecodeError> {
    proto_value::WorkflowExceptionValue::decode(bytes)
}
