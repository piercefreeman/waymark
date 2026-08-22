//! [`Convert`](waymark_convert_core::Convert) implementations between the
//! Python flavor's proto values and VM values.
//!
//! The proto value tree is this flavor's current encoding of a single
//! value, so the conversion is direct: nothing stands between a
//! [`ReadyValue`] and the [`Value`] carrying it.
//!
//! [`Value`]: proto_value::Value
//!
//! # Fidelity
//!
//! The proto value roster is wider than the VM's, so reading collapses
//! what the VM cannot hold: a tuple becomes a list, and a basemodel
//! becomes its data dict, dropping the defining module and name.  Both
//! await a VM value that can express them.
//!
//! A double that is not finite has no [`ReadyValue::Float`] to land in —
//! that variant is a `NonNaNFinite` — so it reads as
//! [`ReadyValue::None`].

#![warn(missing_docs)]

use std::convert::Infallible;

use indexmap::IndexMap;
use typed_floats::NonNaNFinite;
use waymark_convert_core::{Convert as _, TryConvert};
use waymark_proto::python_value as proto_value;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_value_python::{ReadyValue, Value};

use waymark_vm_value_convert_core::PendingPromiseError;

/// Stateless converter with all proto-to-VM value conversion impls.
pub struct Converter;

fn primitive(kind: proto_value::primitive_value::Kind) -> proto_value::value::Kind {
    proto_value::value::Kind::Primitive(proto_value::PrimitiveValue { kind: Some(kind) })
}

impl TryConvert<&ReadyValue, proto_value::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(value: &ReadyValue) -> Result<proto_value::Value, Self::Error> {
        use proto_value::primitive_value::Kind as PrimitiveKind;
        use proto_value::value::Kind;

        let kind = match value {
            ReadyValue::Int(value) => primitive(PrimitiveKind::IntValue(*value)),
            ReadyValue::Float(value) => primitive(PrimitiveKind::DoubleValue(value.get())),
            ReadyValue::Bool(value) => primitive(PrimitiveKind::BoolValue(*value)),
            ReadyValue::String(value) => primitive(PrimitiveKind::StringValue(value.clone())),
            ReadyValue::None => primitive(PrimitiveKind::NullValue(0)),
            ReadyValue::List(items) => Kind::ListValue(proto_value::ListValue {
                items: items
                    .iter()
                    .map(Self::try_convert)
                    .collect::<Result<_, _>>()?,
            }),
            ReadyValue::Dict(entries) => Kind::DictValue(proto_value::DictValue {
                entries: entries
                    .iter()
                    .map(|(key, value)| {
                        Ok(proto_value::DictEntry {
                            key: key.clone(),
                            value: Some(Self::try_convert(value)?),
                        })
                    })
                    .collect::<Result<_, _>>()?,
            }),
            ReadyValue::Exception(exception) => {
                Kind::Exception(Box::new(Self::try_convert(&**exception)?))
            }
            // This flavor has no extension value: the type is
            // uninhabited, so there is nothing to write.
            ReadyValue::Extension(extension) => match *extension {},
        };

        Ok(proto_value::Value { kind: Some(kind) })
    }
}

impl TryConvert<&Value, proto_value::Value> for Converter {
    type Error = PendingPromiseError;

    fn try_convert(value: &Value) -> Result<proto_value::Value, Self::Error> {
        match value {
            Value::Ready(value) => Self::try_convert(value),
            Value::Pending(promise_state_id) => Err(PendingPromiseError(*promise_state_id)),
        }
    }
}

/// Write an exception, whatever its details are a value of.
///
/// The details ride as an ordinary value, so this holds for both the
/// promise-aware [`Value`] an exception carries inside a value tree and
/// the [`ReadyValue`] a settled outcome carries.
impl<'d, Details> TryConvert<&'d Exception<Details>, proto_value::ExceptionValue> for Converter
where
    Converter: TryConvert<&'d Details, proto_value::Value>,
{
    type Error = <Converter as TryConvert<&'d Details, proto_value::Value>>::Error;

    fn try_convert(
        exception: &'d Exception<Details>,
    ) -> Result<proto_value::ExceptionValue, Self::Error> {
        Ok(proto_value::ExceptionValue {
            type_id: exception.type_id.clone(),
            details: Some(Box::new(Self::try_convert(&exception.details)?)),
        })
    }
}

impl TryConvert<&proto_value::Value, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(value: &proto_value::Value) -> Result<ReadyValue, Self::Error> {
        use proto_value::value::Kind;

        let Some(kind) = &value.kind else {
            // A value naming no kind is as empty as the encoding can be.
            return Ok(ReadyValue::None);
        };

        Ok(match kind {
            Kind::Primitive(value) => Self::convert(value),
            // The VM has no value for a basemodel, so it arrives as the
            // data it carries; the defining module and name are dropped.
            Kind::Basemodel(basemodel) => match &basemodel.data {
                Some(data) => Self::convert(data),
                None => ReadyValue::Dict(Default::default()),
            },
            Kind::Exception(exception) => {
                ReadyValue::Exception(Box::new(Self::convert(&**exception)))
            }
            Kind::ListValue(list) => ReadyValue::List(items(&list.items)),
            // The VM has no tuple value, so a tuple arrives as a list.
            Kind::TupleValue(tuple) => ReadyValue::List(items(&tuple.items)),
            Kind::DictValue(dict) => Self::convert(dict),
        })
    }
}

fn items(items: &[proto_value::Value]) -> Vec<Value> {
    items.iter().map(Converter::convert).collect()
}

impl TryConvert<&proto_value::Value, Value> for Converter {
    type Error = Infallible;

    fn try_convert(value: &proto_value::Value) -> Result<Value, Self::Error> {
        Ok(Value::Ready(Self::convert(value)))
    }
}

impl TryConvert<&proto_value::PrimitiveValue, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(value: &proto_value::PrimitiveValue) -> Result<ReadyValue, Self::Error> {
        use proto_value::primitive_value::Kind;

        let Some(kind) = &value.kind else {
            return Ok(ReadyValue::None);
        };

        Ok(match kind {
            Kind::StringValue(value) => ReadyValue::String(value.clone()),
            Kind::DoubleValue(value) => match NonNaNFinite::try_from(*value) {
                Ok(value) => ReadyValue::Float(value),
                // The VM's float is finite and not NaN; anything else has
                // nowhere to land.
                Err(_) => ReadyValue::None,
            },
            Kind::IntValue(value) => ReadyValue::Int(*value),
            Kind::BoolValue(value) => ReadyValue::Bool(*value),
            Kind::NullValue(_) => ReadyValue::None,
        })
    }
}

impl TryConvert<&proto_value::DictValue, ReadyValue> for Converter {
    type Error = Infallible;

    fn try_convert(dict: &proto_value::DictValue) -> Result<ReadyValue, Self::Error> {
        let mut map = IndexMap::with_capacity(dict.entries.len());
        for entry in &dict.entries {
            let value = match &entry.value {
                Some(value) => Self::convert(value),
                None => Value::Ready(ReadyValue::None),
            };
            map.insert(entry.key.clone(), value);
        }
        Ok(ReadyValue::Dict(map))
    }
}

/// Read an exception into whatever its details are a value of.
///
/// An exception naming no details carries the value the flavor has for
/// nothing at all.
impl<'d, Details> TryConvert<&'d proto_value::ExceptionValue, Exception<Details>> for Converter
where
    Converter: TryConvert<&'d proto_value::Value, Details, Error = Infallible>,
    Details: From<ReadyValue>,
{
    type Error = Infallible;

    fn try_convert(
        exception: &'d proto_value::ExceptionValue,
    ) -> Result<Exception<Details>, Self::Error> {
        Ok(Exception {
            type_id: exception.type_id.clone(),
            details: exception
                .details
                .as_deref()
                .map(Self::convert)
                .unwrap_or_else(|| ReadyValue::None.into()),
        })
    }
}

/// Read a ready value back from an owned encoded-value payload.
impl TryConvert<Vec<u8>, ReadyValue> for Converter {
    type Error = prost::DecodeError;

    fn try_convert(bytes: Vec<u8>) -> Result<ReadyValue, Self::Error> {
        let message: proto_value::Value = prost::Message::decode(bytes.as_slice())?;
        Ok(Self::convert(&message))
    }
}

/// Read an exception back from an owned payload of its own wire
/// message.
///
/// The exception payload is carried as an encoded
/// [`proto_value::ExceptionValue`], a message of its own rather than a
/// value.
impl TryConvert<Vec<u8>, Exception<ReadyValue>> for Converter {
    type Error = prost::DecodeError;

    fn try_convert(bytes: Vec<u8>) -> Result<Exception<ReadyValue>, Self::Error> {
        let message: proto_value::ExceptionValue = prost::Message::decode(bytes.as_slice())?;
        Ok(Self::convert(&message))
    }
}

/// An argument entry named a key but carried no value, which is a
/// malformed arguments message rather than an argument that holds
/// nothing (this flavor's "nothing" is an encoded `None`).
#[derive(Debug, thiserror::Error)]
#[error("argument {key:?} carries no value")]
pub struct MissingArgumentValueError {
    /// The framing key of the value-less entry.
    pub key: String,
}

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

/// Read named-argument entries — the `(key, value)` shape both argument
/// messages share — into the map of ready values they carry.
///
/// An entry carrying no value is a [`MissingArgumentValueError`].
fn named_arguments<'a>(
    entries: impl Iterator<Item = (&'a String, Option<&'a proto_value::Value>)>,
) -> Result<std::collections::HashMap<String, ReadyValue>, MissingArgumentValueError> {
    entries
        .map(|(key, value)| {
            let value = value.ok_or_else(|| MissingArgumentValueError { key: key.clone() })?;
            Ok((key.clone(), Converter::convert(value)))
        })
        .collect()
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
impl TryConvert<(&[u8], &[String]), Vec<Value>> for Converter {
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
    for Converter
{
    type Error = PendingPromiseError;

    fn try_convert(
        outcome: waymark_workflow_completion_core::Outcome<ReadyValue>,
    ) -> Result<proto_value::WorkflowOutcome, Self::Error> {
        use proto_value::workflow_outcome::Outcome;

        let outcome = match outcome {
            waymark_workflow_completion_core::Outcome::Completion(value) => {
                Outcome::Value(Self::try_convert(&value)?)
            }
            waymark_workflow_completion_core::Outcome::Exception(exception) => {
                Outcome::Exception(Self::try_convert(&exception)?)
            }
        };

        Ok(proto_value::WorkflowOutcome {
            outcome: Some(outcome),
        })
    }
}

/// Convert how a workflow completed into the bytes the completion
/// payload carries: the outcome message, encoded.
impl TryConvert<waymark_workflow_completion_core::Outcome<ReadyValue>, Vec<u8>> for Converter {
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
    use super::*;

    fn ready(value: ReadyValue) -> Value {
        Value::Ready(value)
    }

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

    fn read(value: &proto_value::Value) -> ReadyValue {
        Converter::convert(value)
    }

    fn round_trip(value: ReadyValue) -> ReadyValue {
        let encoded: proto_value::Value =
            Converter::try_convert(&value).expect("no pending promise in the value");
        Converter::convert(&encoded)
    }

    #[test]
    fn primitives_round_trip() {
        for value in [
            ReadyValue::Int(42),
            ReadyValue::Int(-7),
            ReadyValue::Float(NonNaNFinite::try_from(1.25).unwrap()),
            ReadyValue::Bool(true),
            ReadyValue::String("hello".to_owned()),
            ReadyValue::None,
        ] {
            assert_eq!(round_trip(value.clone()), value);
        }
    }

    #[test]
    fn nested_containers_round_trip() {
        let value = ReadyValue::Dict(IndexMap::from([
            (
                "list".to_owned(),
                ready(ReadyValue::List(vec![
                    ready(ReadyValue::Int(1)),
                    ready(ReadyValue::String("two".to_owned())),
                ])),
            ),
            (
                "nested".to_owned(),
                ready(ReadyValue::Dict(IndexMap::from([(
                    "deep".to_owned(),
                    ready(ReadyValue::Bool(false)),
                )]))),
            ),
        ]));

        assert_eq!(round_trip(value.clone()), value);
    }

    #[test]
    fn dict_keeps_its_insertion_order() {
        // The order a dict was built in is part of the value: Python
        // dicts are insertion-ordered.  Sorting the keys en route — as a
        // JSON intermediary would — is a corruption, so the order has to
        // survive a round trip in the order given, not in the order the
        // keys happen to sort in.
        let keys = ["zebra", "apple", "mango", "banana"];
        let value = ReadyValue::Dict(
            keys.iter()
                .enumerate()
                .map(|(index, key)| ((*key).to_owned(), ready(ReadyValue::Int(index as i64))))
                .collect(),
        );

        let ReadyValue::Dict(entries) = round_trip(value) else {
            panic!("a dict round trips as a dict");
        };

        let round_tripped: Vec<_> = entries.keys().map(String::as_str).collect();
        assert_eq!(round_tripped, keys);
    }

    #[test]
    fn an_exception_stays_an_exception() {
        // The exception-ness is the point: it is what lets the VM settle
        // a promise raised rather than with a value.
        let exception = ReadyValue::Exception(Box::new(Exception {
            type_id: "ValueError".to_owned(),
            details: ready(ReadyValue::Dict(IndexMap::from([(
                "message".to_owned(),
                ready(ReadyValue::String("boom".to_owned())),
            )]))),
        }));

        assert_eq!(round_trip(exception.clone()), exception);
    }

    #[test]
    fn a_tuple_reads_as_a_list() {
        let tuple = proto_value::Value {
            kind: Some(proto_value::value::Kind::TupleValue(
                proto_value::TupleValue {
                    items: vec![
                        Converter::try_convert(&ReadyValue::Int(1)).unwrap(),
                        Converter::try_convert(&ReadyValue::Int(2)).unwrap(),
                    ],
                },
            )),
        };

        assert_eq!(
            read(&tuple),
            ReadyValue::List(vec![ready(ReadyValue::Int(1)), ready(ReadyValue::Int(2))]),
        );
    }

    #[test]
    fn a_basemodel_reads_as_its_data() {
        let basemodel = proto_value::Value {
            kind: Some(proto_value::value::Kind::Basemodel(
                proto_value::BaseModelValue {
                    module: "example".to_owned(),
                    name: "Model".to_owned(),
                    data: Some(proto_value::DictValue {
                        entries: vec![proto_value::DictEntry {
                            key: "field".to_owned(),
                            value: Some(Converter::try_convert(&ReadyValue::Int(3)).unwrap()),
                        }],
                    }),
                },
            )),
        };

        assert_eq!(
            read(&basemodel),
            ReadyValue::Dict(IndexMap::from([(
                "field".to_owned(),
                ready(ReadyValue::Int(3)),
            )])),
        );
    }

    #[test]
    fn a_non_finite_double_reads_as_none() {
        for double in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let value = proto_value::Value {
                kind: Some(primitive(proto_value::primitive_value::Kind::DoubleValue(
                    double,
                ))),
            };

            assert_eq!(read(&value), ReadyValue::None);
        }
    }

    #[test]
    fn an_encoded_value_payload_reads_back() {
        // A value payload IS an encoded proto value message, byte for
        // byte — what the framing-level `WorkflowArgument.value`
        // carries.
        let value = ReadyValue::Dict(IndexMap::from([
            ("zebra".to_owned(), ready(ReadyValue::Int(1))),
            ("apple".to_owned(), ready(ReadyValue::Int(2))),
        ]));

        let message: proto_value::Value =
            Converter::try_convert(&value).expect("no pending promise");
        let bytes = prost::Message::encode_to_vec(&message);

        let read: ReadyValue = Converter::try_convert(bytes).expect("the bytes decode");
        assert_eq!(read, value);
    }

    #[test]
    fn a_pending_promise_cannot_be_written() {
        let value = ReadyValue::List(vec![Value::Pending(
            waymark_vm_runtime_promise_core::PromiseStateId(7),
        )]);

        let written: Result<proto_value::Value, _> = Converter::try_convert(&value);

        assert_eq!(
            written.expect_err("a pending promise has no encoding"),
            PendingPromiseError(waymark_vm_runtime_promise_core::PromiseStateId(7)),
        );
    }
}
