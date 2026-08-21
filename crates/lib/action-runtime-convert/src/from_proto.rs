use waymark_convert_core::{Convert as _, TryConvert};

use crate::{ActionResultError, Converter, MissingOutcomeError};

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
        let action_outcome: waymark_proto::python_value::ActionOutcome =
            waymark_vm_value_python_convert_proto::Converter::try_convert(
                result.payload.as_slice(),
            )
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
                waymark_vm_value_python_convert_proto::Converter::convert(&value),
            ),
            Outcome::Exception(exception) => {
                waymark_action_runtime_core::ActionCallOutcome::Exception(
                    waymark_vm_value_python_convert_proto::Converter::convert(&exception),
                )
            }
        };

        Ok(outcome)
    }
}

/// Convert how an action call completed into the flavor's outcome
/// message: a returned value in the `value` arm, a raised exception in
/// the `exception` arm.
impl
    TryConvert<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
        waymark_proto::python_value::ActionOutcome,
    > for Converter
{
    type Error = waymark_vm_value_convert_core::PendingPromiseError;

    fn try_convert(
        outcome: waymark_action_runtime_core::ActionCallOutcome<
            waymark_vm_value_python::ReadyValue,
        >,
    ) -> Result<waymark_proto::python_value::ActionOutcome, Self::Error> {
        use waymark_proto::python_value::action_outcome::Outcome;

        let outcome = match outcome {
            waymark_action_runtime_core::ActionCallOutcome::Value(value) => Outcome::Value(
                waymark_vm_value_python_convert_proto::Converter::try_convert(&value)?,
            ),
            waymark_action_runtime_core::ActionCallOutcome::Exception(exception) => {
                Outcome::Exception(
                    waymark_vm_value_python_convert_proto::Converter::try_convert(&exception)?,
                )
            }
        };

        Ok(waymark_proto::python_value::ActionOutcome {
            outcome: Some(outcome),
        })
    }
}

/// Convert how an action call completed into the bytes the result
/// payload carries: the outcome message, encoded.
impl
    TryConvert<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
        Vec<u8>,
    > for Converter
{
    type Error = waymark_vm_value_convert_core::PendingPromiseError;

    fn try_convert(
        outcome: waymark_action_runtime_core::ActionCallOutcome<
            waymark_vm_value_python::ReadyValue,
        >,
    ) -> Result<Vec<u8>, Self::Error> {
        let message: waymark_proto::python_value::ActionOutcome = Self::try_convert(outcome)?;
        let Ok(bytes) = waymark_vm_value_python_convert_proto::Converter::try_convert(&message);
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action_result(payload: Vec<u8>) -> waymark_proto::messages::ActionResult {
        waymark_proto::messages::ActionResult {
            payload,
            ..Default::default()
        }
    }

    fn payload(
        outcome: waymark_action_runtime_core::ActionCallOutcome<
            waymark_vm_value_python::ReadyValue,
        >,
    ) -> Vec<u8> {
        Converter::try_convert(outcome).expect("no pending promise in the outcome")
    }

    fn outcome(
        result: &waymark_proto::messages::ActionResult,
    ) -> waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue> {
        Converter::try_convert(result).expect("the encoded value decodes")
    }

    #[test]
    fn returned_value_becomes_the_settled_value() {
        let returned = payload(waymark_action_runtime_core::ActionCallOutcome::Value(
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
        let exception = waymark_vm_runtime_exception::Exception {
            type_id: "ValueError".to_owned(),
            details: waymark_vm_value_python::Value::Ready(
                waymark_vm_value_python::ReadyValue::String("boom".to_owned()),
            ),
        };
        let returned = payload(waymark_action_runtime_core::ActionCallOutcome::Value(
            waymark_vm_value_python::ReadyValue::Exception(Box::new(exception)),
        ));

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
        let raised = payload(waymark_action_runtime_core::ActionCallOutcome::Exception(
            waymark_vm_runtime_exception::Exception {
                type_id: "RetryCounterError".to_owned(),
                details: waymark_vm_value_python::ReadyValue::Dict(indexmap::IndexMap::from([(
                    "message".to_owned(),
                    waymark_vm_value_python::Value::Ready(
                        waymark_vm_value_python::ReadyValue::String(
                            "attempt 1 has not reached success".to_owned(),
                        ),
                    ),
                )])),
            },
        ));

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
