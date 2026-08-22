//! Conversions of the workflow's initiation arguments and completion
//! outcomes.

use waymark_convert_core::TryConvert;
use waymark_proto::python_value as proto_value;
use waymark_vm_value_python::{ReadyValue, Value};

use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;
use crate::common::{MissingArgumentValueError, named_arguments};

/// Stateless converter for the workflow initiation arguments: argument
/// payloads read into the entry function's positional arguments.
pub struct WorkflowArgumentsConverter;

/// Stateless converter for the workflow completion outcome: outcomes
/// written as this flavor's messages and payloads.
pub struct WorkflowOutcomeConverter;

/// Error reading the positional entry-function arguments a
/// workflow-arguments payload encodes.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowArgumentsError {
    /// The payload's bytes do not decode as this flavor's workflow
    /// arguments message.
    #[error("decoding the workflow arguments: {0}")]
    Decode(#[source] prost::DecodeError),

    /// The decoded arguments are malformed.
    #[error("reading the workflow arguments: {0}")]
    Arguments(#[source] MissingArgumentValueError),
}

/// Convert a workflow-arguments payload and the entry function's
/// ordered input names into positional arguments.
///
/// This flavor's initiation convention: named arguments are matched to
/// inputs by name; inputs the payload does not name default to this
/// language's nothing value.  An empty payload (the no-arguments
/// encoding) defaults every input.
impl TryConvert<(&[u8], &[String]), Vec<Value>> for WorkflowArgumentsConverter {
    type Error = WorkflowArgumentsError;

    fn try_convert(
        (arguments, input_names): (&[u8], &[String]),
    ) -> Result<Vec<Value>, Self::Error> {
        let message: proto_value::WorkflowArguments =
            prost::Message::decode(arguments).map_err(WorkflowArgumentsError::Decode)?;
        let args_map = named_arguments(
            message
                .arguments
                .iter()
                .map(|argument| (&argument.key, argument.value.as_ref())),
        )
        .map_err(WorkflowArgumentsError::Arguments)?;

        Ok(input_names
            .iter()
            .map(|name| {
                args_map
                    .get(name)
                    .cloned()
                    .map(Value::Ready)
                    .unwrap_or(Value::Ready(ReadyValue::None))
            })
            .collect())
    }
}

/// Convert how a workflow completed into this flavor's outcome message:
/// the returned value in the `value` arm, the ending exception in the
/// `exception` arm.
impl TryConvert<waymark_workflow_completion_core::Outcome<ReadyValue>, proto_value::WorkflowOutcome>
    for WorkflowOutcomeConverter
{
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: waymark_workflow_completion_core::Outcome<ReadyValue>,
    ) -> Result<proto_value::WorkflowOutcome, Self::Error> {
        use proto_value::workflow_outcome::Outcome;

        let outcome = match outcome {
            waymark_workflow_completion_core::Outcome::Completion(value) => {
                Outcome::Value(Converter::try_convert(&value)?)
            }
            waymark_workflow_completion_core::Outcome::Exception(exception) => {
                Outcome::Exception(Converter::try_convert(&exception)?)
            }
        };

        Ok(proto_value::WorkflowOutcome {
            outcome: Some(outcome),
        })
    }
}

/// Convert how a workflow completed into the bytes the completion
/// payload carries: the outcome message, encoded.
impl TryConvert<waymark_workflow_completion_core::Outcome<ReadyValue>, Vec<u8>>
    for WorkflowOutcomeConverter
{
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: waymark_workflow_completion_core::Outcome<ReadyValue>,
    ) -> Result<Vec<u8>, Self::Error> {
        let message: proto_value::WorkflowOutcome = Self::try_convert(outcome)?;
        Ok(prost::Message::encode_to_vec(&message))
    }
}

#[cfg(test)]
mod tests {
    use proto_value::workflow_outcome::Outcome as ProtoOutcome;
    use waymark_workflow_completion_core::Outcome;

    use super::*;

    #[test]
    fn a_completion_writes_the_value_arm() {
        use proto_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

        let message: proto_value::WorkflowOutcome =
            WorkflowOutcomeConverter::try_convert(Outcome::Completion(ReadyValue::Int(42)))
                .expect("no pending promise");

        // The returned value travels as-is in the `value` arm: no
        // envelope around it.
        let Some(ProtoOutcome::Value(value)) = &message.outcome else {
            panic!("a completion is the value arm, got {message:?}");
        };
        let Some(Kind::Primitive(primitive)) = &value.kind else {
            panic!("the value is a primitive, got {value:?}");
        };
        assert_eq!(primitive.kind, Some(PrimitiveKind::IntValue(42)));
    }

    #[test]
    fn an_exception_writes_the_exception_arm() {
        use proto_value::{primitive_value::Kind as PrimitiveKind, value::Kind};

        let exception = waymark_vm_runtime_exception::Exception {
            type_id: "ValueError".to_owned(),
            details: ReadyValue::String("boom".to_owned()),
        };
        let message: proto_value::WorkflowOutcome =
            WorkflowOutcomeConverter::try_convert(Outcome::Exception(exception))
                .expect("no pending promise");

        // The exception arm is the exception itself: the type id and the
        // details it carries.
        let Some(ProtoOutcome::Exception(exception)) = &message.outcome else {
            panic!("an exception is the exception arm, got {message:?}");
        };
        assert_eq!(exception.type_id, "ValueError");
        let Some(Kind::Primitive(primitive)) = exception
            .details
            .as_ref()
            .and_then(|details| details.kind.as_ref())
        else {
            panic!("the details are a primitive, got {exception:?}");
        };
        assert_eq!(
            primitive.kind,
            Some(PrimitiveKind::StringValue("boom".to_owned()))
        );
    }

    #[test]
    fn an_outcome_payload_is_the_encoded_message() {
        let bytes: Vec<u8> =
            WorkflowOutcomeConverter::try_convert(Outcome::Completion(ReadyValue::Int(42)))
                .expect("no pending promise");
        let message: proto_value::WorkflowOutcome =
            WorkflowOutcomeConverter::try_convert(Outcome::Completion(ReadyValue::Int(42)))
                .expect("no pending promise");

        assert_eq!(bytes, prost::Message::encode_to_vec(&message));
    }
}
