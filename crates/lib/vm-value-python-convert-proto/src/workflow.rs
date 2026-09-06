//! Conversions of the workflow seams: initiation arguments and
//! completion outcomes.

use waymark_convert_core::TryConvert;
use waymark_proto::python_value as proto_value;
use waymark_vm_value_python::{ReadyValue, Value};

use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;
use crate::common::{MissingArgumentValueError, named_arguments};

/// Stateless converter for the workflow initiation seam: argument
/// payloads read into the entry function's positional arguments.
pub struct WorkflowArgumentsConverter;

/// Stateless converter for the workflow completion seam: outcomes
/// written as this flavor's messages and payloads.
pub struct WorkflowOutcomeConverter;

/// Error reading the positional entry-function arguments a
/// workflow-arguments payload encodes.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowArgumentsError {
    /// The payload's bytes do not decode as this flavor's workflow
    /// arguments message.
    #[error("decoding the workflow arguments")]
    Decode(#[source] prost::DecodeError),

    /// The decoded arguments are malformed.
    #[error("reading the workflow arguments")]
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
