//! Conversions of the action-call seam: dispatch arguments, result
//! outcomes, and loss rendering.

use std::convert::Infallible;

use waymark_convert_core::{Convert as _, TryConvert};
use waymark_proto::python_value as proto_value;
use waymark_vm_value_python::{ReadyValue, Value};

use waymark_vm_value_convert_core::PendingPromiseError;

use crate::Converter;
use crate::common::{MissingArgumentValueError, named_arguments};

/// Convert a pair of call-argument names and values straight into the
/// bytes the dispatch carries: the arguments message, encoded.
///
/// This is the flavor's calling convention: `call_args` names from the
/// `ActionRef` paired positionally with the argument values from the
/// VM into named arguments, in pairing order.
///
/// No arguments encode as no bytes — an entry-less message has the
/// empty encoding, so "empty payload means no arguments" needs no case
/// of its own.
impl TryConvert<(Vec<String>, Vec<ReadyValue>), Vec<u8>> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(
        (names, values): (Vec<String>, Vec<ReadyValue>),
    ) -> Result<Vec<u8>, Self::Error> {
        let mut arguments = Vec::with_capacity(names.len());
        for (name, value) in names.iter().zip(values.iter()) {
            // Skip `None`-valued parameters (dependency markers such as
            // `Annotated[T, Depend(…)]` are serialized as `None` by the
            // VM).  The Python side (`provide_dependencies`) will resolve
            // them from the function signature instead.
            if matches!(value, ReadyValue::None) {
                continue;
            }
            let value: proto_value::Value = Self::try_convert(value)?;
            arguments.push(proto_value::ActionArgument {
                key: name.clone(),
                value: Some(value),
            });
        }
        let message = proto_value::ActionArguments { arguments };
        Ok(prost::Message::encode_to_vec(&message))
    }
}

/// The result named no outcome, so the worker never said how the call
/// completed — neither a returned value nor a raised exception, which is
/// a worker that violated the protocol rather than a call that produced
/// nothing.
#[derive(Debug, thiserror::Error)]
#[error("the action result names no outcome")]
pub struct MissingOutcomeError;

/// Error reading the outcome an action-result payload encodes.
#[derive(Debug, thiserror::Error)]
pub enum ActionOutcomeError {
    /// The payload's bytes do not decode as this flavor's outcome
    /// message.
    #[error("decoding the action outcome")]
    Decode(#[source] prost::DecodeError),

    /// The decoded outcome did not say how the call completed.
    #[error("reading the action outcome's arms")]
    Outcome(#[source] MissingOutcomeError),
}

/// Convert how an action call completed into the outcome that settles
/// the awaiting promise.
///
/// The outcome is optional as the encoding leaves it so, not as a state
/// the protocol admits: a result naming neither arm is rejected rather
/// than settled with a stand-in value.
impl
    TryConvert<
        Option<proto_value::action_outcome::Outcome>,
        waymark_action_runtime_core::ActionCallOutcome<ReadyValue>,
    > for Converter
{
    type Error = MissingOutcomeError;

    fn try_convert(
        outcome: Option<proto_value::action_outcome::Outcome>,
    ) -> Result<waymark_action_runtime_core::ActionCallOutcome<ReadyValue>, Self::Error> {
        use proto_value::action_outcome::Outcome;

        let outcome = match outcome.ok_or(MissingOutcomeError)? {
            Outcome::Value(value) => {
                waymark_action_runtime_core::ActionCallOutcome::Value(Self::convert(&value))
            }
            Outcome::Exception(exception) => {
                waymark_action_runtime_core::ActionCallOutcome::Exception(Self::convert(&exception))
            }
        };

        Ok(outcome)
    }
}

/// Read how an action call completed from the result payload: the
/// outcome message, decoded and interpreted.
impl TryConvert<Vec<u8>, waymark_action_runtime_core::ActionCallOutcome<ReadyValue>> for Converter {
    type Error = ActionOutcomeError;

    fn try_convert(
        bytes: Vec<u8>,
    ) -> Result<waymark_action_runtime_core::ActionCallOutcome<ReadyValue>, Self::Error> {
        let message: proto_value::ActionOutcome =
            prost::Message::decode(bytes.as_slice()).map_err(ActionOutcomeError::Decode)?;
        Self::try_convert(message.outcome).map_err(ActionOutcomeError::Outcome)
    }
}

/// Convert how an action call completed into the flavor's outcome
/// message: a returned value in the `value` arm, a raised exception in
/// the `exception` arm.
impl
    TryConvert<
        waymark_action_runtime_core::ActionCallOutcome<ReadyValue>,
        proto_value::ActionOutcome,
    > for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: waymark_action_runtime_core::ActionCallOutcome<ReadyValue>,
    ) -> Result<proto_value::ActionOutcome, Self::Error> {
        use proto_value::action_outcome::Outcome;

        let outcome = match outcome {
            waymark_action_runtime_core::ActionCallOutcome::Value(value) => {
                Outcome::Value(Self::try_convert(&value)?)
            }
            waymark_action_runtime_core::ActionCallOutcome::Exception(exception) => {
                Outcome::Exception(Self::try_convert(&exception)?)
            }
        };

        Ok(proto_value::ActionOutcome {
            outcome: Some(outcome),
        })
    }
}

/// Convert how an action call completed into the bytes the result
/// payload carries: the outcome message, encoded.
impl TryConvert<waymark_action_runtime_core::ActionCallOutcome<ReadyValue>, Vec<u8>> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: waymark_action_runtime_core::ActionCallOutcome<ReadyValue>,
    ) -> Result<Vec<u8>, Self::Error> {
        let message: proto_value::ActionOutcome = Self::try_convert(outcome)?;
        Ok(prost::Message::encode_to_vec(&message))
    }
}

/// Render an action-call loss as this flavor's exception details: a
/// dict carrying the stage the call provably reached.
///
/// The loss semantics — that a loss settles the promise raised as
/// EXECUTION_LOST — belong to the action runtime's converter; this
/// flavor only states the fact in its own vocabulary.
impl TryConvert<waymark_action_runtime_core::ActionCallLossError, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(
        loss: waymark_action_runtime_core::ActionCallLossError,
    ) -> Result<ReadyValue, Self::Error> {
        let stage = match loss.stage {
            waymark_action_runtime_core::ActionCallStage::NotStarted => "not_started",
            waymark_action_runtime_core::ActionCallStage::Unknown => "unknown",
        };

        Ok(ReadyValue::Dict(
            [(
                "stage".to_owned(),
                Value::Ready(ReadyValue::String(stage.to_owned())),
            )]
            .into_iter()
            .collect(),
        ))
    }
}

/// Error reading the named values an action-arguments payload encodes.
#[derive(Debug, thiserror::Error)]
pub enum ActionArgumentsError {
    /// The payload's bytes do not decode as this flavor's arguments
    /// message.
    #[error("decoding the action arguments")]
    Decode(#[source] prost::DecodeError),

    /// The decoded arguments are malformed.
    #[error("reading the action arguments")]
    Arguments(#[source] MissingArgumentValueError),
}

/// Read the named argument values from an owned action-arguments
/// payload: the arguments message, decoded into the map an action body
/// is called with.
///
/// Empty bytes are the no-arguments encoding and read as no values.
impl TryConvert<Vec<u8>, std::collections::HashMap<String, ReadyValue>> for Converter {
    type Error = ActionArgumentsError;

    fn try_convert(
        bytes: Vec<u8>,
    ) -> Result<std::collections::HashMap<String, ReadyValue>, Self::Error> {
        let message: proto_value::ActionArguments =
            prost::Message::decode(bytes.as_slice()).map_err(ActionArgumentsError::Decode)?;
        named_arguments(
            message
                .arguments
                .iter()
                .map(|argument| (&argument.key, argument.value.as_ref())),
        )
        .map_err(ActionArgumentsError::Arguments)
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use waymark_vm_runtime_exception::Exception;

    use super::*;

    fn outcome_payload(
        outcome: waymark_action_runtime_core::ActionCallOutcome<ReadyValue>,
    ) -> Vec<u8> {
        Converter::try_convert(outcome).expect("no pending promise in the outcome")
    }

    fn read_outcome(payload: &[u8]) -> waymark_action_runtime_core::ActionCallOutcome<ReadyValue> {
        Converter::try_convert(payload.to_vec()).expect("the encoded outcome decodes")
    }

    #[test]
    fn returned_value_becomes_the_settled_value() {
        let returned = outcome_payload(waymark_action_runtime_core::ActionCallOutcome::Value(
            ReadyValue::Int(42),
        ));

        let waymark_action_runtime_core::ActionCallOutcome::Value(value) = read_outcome(&returned)
        else {
            panic!("a returned value must settle the promise with that value");
        };
        assert_eq!(value, ReadyValue::Int(42));
    }

    #[test]
    fn returned_exception_settles_as_an_ordinary_value() {
        // Returning an exception is not raising it: it arrives as a value.
        let exception = Exception {
            type_id: "ValueError".to_owned(),
            details: Value::Ready(ReadyValue::String("boom".to_owned())),
        };
        let returned = outcome_payload(waymark_action_runtime_core::ActionCallOutcome::Value(
            ReadyValue::Exception(Box::new(exception)),
        ));

        assert!(matches!(
            read_outcome(&returned),
            waymark_action_runtime_core::ActionCallOutcome::Value(_)
        ));
    }

    #[test]
    fn a_payload_naming_no_outcome_is_an_error() {
        // A worker that says nothing about how the call completed is
        // broken; settling the promise with some stand-in value would
        // bury that.
        let empty = prost::Message::encode_to_vec(&proto_value::ActionOutcome { outcome: None });

        let converted: Result<waymark_action_runtime_core::ActionCallOutcome<ReadyValue>, _> =
            Converter::try_convert(empty);

        assert!(
            matches!(converted, Err(ActionOutcomeError::Outcome(_))),
            "a payload naming no outcome cannot settle a promise",
        );
    }

    #[test]
    fn raised_exception_keeps_its_type_id_and_details() {
        let raised = outcome_payload(waymark_action_runtime_core::ActionCallOutcome::Exception(
            Exception {
                type_id: "RetryCounterError".to_owned(),
                details: ReadyValue::Dict(IndexMap::from([(
                    "message".to_owned(),
                    Value::Ready(ReadyValue::String(
                        "attempt 1 has not reached success".to_owned(),
                    )),
                )])),
            },
        ));

        let waymark_action_runtime_core::ActionCallOutcome::Exception(exception) =
            read_outcome(&raised)
        else {
            panic!("a raised exception must settle the promise with an exception");
        };

        assert_eq!(exception.type_id, "RetryCounterError");
        let ReadyValue::Dict(details) = exception.details else {
            panic!("the exception's details are its own value");
        };
        assert_eq!(
            details.get("message"),
            Some(&Value::Ready(ReadyValue::String(
                "attempt 1 has not reached success".to_owned()
            ))),
        );
    }

    #[test]
    fn a_loss_renders_as_stage_details() {
        let details: ReadyValue =
            Converter::convert(waymark_action_runtime_core::ActionCallLossError {
                stage: waymark_action_runtime_core::ActionCallStage::NotStarted,
            });

        let ReadyValue::Dict(entries) = details else {
            panic!("the loss details are a dict");
        };
        assert_eq!(
            entries.get("stage"),
            Some(&Value::Ready(ReadyValue::String("not_started".to_owned()))),
        );
    }
}
