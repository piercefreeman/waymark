use waymark_convert_core::{Convert as _, TryConvert};

use crate::{ActionResultError, Converter, MissingOutcomeError};

/// Convert proto workflow arguments back into a VM ready value.
///
/// The framing-level argument names become the keys of a
/// [`waymark_vm_value_python::ReadyValue::Dict`], each carrying the value
/// its opaque bytes decode to.  The names keep the order the framing
/// gave them.
///
/// Fallible since the framing-level arguments carry their values as
/// opaque encoded values that decode here.
impl TryConvert<&waymark_proto::messages::WorkflowArguments, waymark_vm_value_python::ReadyValue>
    for Converter
{
    type Error = crate::DecodeArgumentError;

    fn try_convert(
        value: &waymark_proto::messages::WorkflowArguments,
    ) -> Result<waymark_vm_value_python::ReadyValue, Self::Error> {
        let mut entries = indexmap::IndexMap::with_capacity(value.arguments.len());
        for argument in &value.arguments {
            let decoded = waymark_proto_python_value_conversions::decode_value(&argument.value)
                .map_err(|source| crate::DecodeArgumentError {
                    key: argument.key.clone(),
                    source,
                })?;
            entries.insert(
                argument.key.clone(),
                waymark_vm_value_convert_proto::Converter::convert(&decoded),
            );
        }

        Ok(waymark_vm_value_python::ReadyValue::Dict(entries))
    }
}

/// Convert an action result into the outcome that settles the awaiting
/// promise.
///
/// The framing carries one opaque payload; how the call completed is
/// the encoded value's own business, so it is read here as the flavor's
/// [`ActionOutcome`](waymark_proto::python_value::ActionOutcome):
/// a returned value settles the promise with that value, a raised
/// exception settles it raised.
impl
    TryConvert<
        &waymark_proto::messages::ActionResult,
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
    > for Converter
{
    type Error = ActionResultError;

    fn try_convert(
        result: &waymark_proto::messages::ActionResult,
    ) -> Result<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
        Self::Error,
    > {
        let action_outcome =
            waymark_proto_python_value_conversions::decode_action_outcome(&result.payload)
                .map_err(ActionResultError::Decode)?;

        Self::try_convert(action_outcome.outcome).map_err(ActionResultError::Outcome)
    }
}

/// Convert how an action call completed into the outcome that settles
/// the awaiting promise.
///
/// The outcome is optional as the encoding leaves it so, not as a state
/// the protocol admits: a result naming neither arm is rejected rather
/// than settled with a stand-in value.
impl
    TryConvert<
        Option<waymark_proto::python_value::action_outcome::Outcome>,
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
    > for Converter
{
    type Error = MissingOutcomeError;

    fn try_convert(
        outcome: Option<waymark_proto::python_value::action_outcome::Outcome>,
    ) -> Result<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
        Self::Error,
    > {
        use waymark_proto::python_value::action_outcome::Outcome;

        let outcome = match outcome.ok_or(MissingOutcomeError)? {
            Outcome::Value(value) => waymark_action_runtime_core::ActionCallOutcome::Value(
                waymark_vm_value_convert_proto::Converter::convert(&value),
            ),
            Outcome::Exception(exception) => {
                waymark_action_runtime_core::ActionCallOutcome::Exception(
                    waymark_vm_value_convert_proto::Converter::convert(&exception),
                )
            }
        };

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action_result(
        action_outcome: waymark_proto::python_value::ActionOutcome,
    ) -> waymark_proto::messages::ActionResult {
        waymark_proto::messages::ActionResult {
            payload: waymark_proto_python_value_conversions::encode_action_outcome(&action_outcome),
            ..Default::default()
        }
    }

    fn outcome(
        result: &waymark_proto::messages::ActionResult,
    ) -> waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue> {
        Converter::try_convert(result).expect("the encoded value decodes")
    }

    fn encoded(value: waymark_vm_value_python::ReadyValue) -> waymark_proto::python_value::Value {
        waymark_vm_value_convert_proto::Converter::try_convert(&value)
            .expect("no pending promise in the value")
    }

    #[test]
    fn returned_value_becomes_the_settled_value() {
        let returned = waymark_proto_python_value_conversions::returned_value(encoded(
            waymark_vm_value_python::ReadyValue::Int(42),
        ));

        let waymark_action_runtime_core::ActionCallOutcome::Value(value) =
            outcome(&action_result(returned))
        else {
            panic!("a returned value must settle the promise with that value");
        };
        assert_eq!(value, waymark_vm_value_python::ReadyValue::Int(42));
    }

    #[test]
    fn returned_exception_settles_as_an_ordinary_value() {
        // Returning an exception is not raising it: it arrives as a value.
        let returned = waymark_proto_python_value_conversions::returned_value(
            waymark_proto::python_value::Value {
                kind: Some(waymark_proto::python_value::value::Kind::Exception(
                    Box::new(waymark_proto_python_value_conversions::exception_value(
                        "ValueError".to_owned(),
                        "boom".to_owned(),
                    )),
                )),
            },
        );

        assert!(matches!(
            outcome(&action_result(returned)),
            waymark_action_runtime_core::ActionCallOutcome::Value(_)
        ));
    }

    #[test]
    fn a_result_naming_no_outcome_is_an_error() {
        // A worker that says nothing about how the call completed is
        // broken; settling the promise with some stand-in value would
        // bury that.
        let result = waymark_proto::messages::ActionResult::default();

        let converted: Result<
            waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
            _,
        > = Converter::try_convert(&result);

        assert!(
            matches!(converted, Err(ActionResultError::Outcome(_))),
            "a result naming no outcome cannot settle a promise",
        );
    }

    #[test]
    fn raised_exception_keeps_its_type_id_and_details() {
        let raised = waymark_proto_python_value_conversions::raised_exception(
            waymark_proto::python_value::ExceptionValue {
                type_id: "RetryCounterError".to_owned(),
                details: Some(Box::new(encoded(
                    waymark_vm_value_python::ReadyValue::Dict(indexmap::IndexMap::from([(
                        "message".to_owned(),
                        waymark_vm_value_python::Value::Ready(
                            waymark_vm_value_python::ReadyValue::String(
                                "attempt 1 has not reached success".to_owned(),
                            ),
                        ),
                    )])),
                ))),
            },
        );

        let waymark_action_runtime_core::ActionCallOutcome::Exception(exception) =
            outcome(&action_result(raised))
        else {
            panic!("a raised exception must settle the promise with an exception");
        };

        assert_eq!(exception.type_id, "RetryCounterError");
        let waymark_vm_value_python::ReadyValue::Dict(details) = exception.details else {
            panic!("the exception's details are its own value");
        };
        assert_eq!(
            details.get("message"),
            Some(&waymark_vm_value_python::Value::Ready(
                waymark_vm_value_python::ReadyValue::String(
                    "attempt 1 has not reached success".to_owned()
                )
            )),
        );
    }
}
