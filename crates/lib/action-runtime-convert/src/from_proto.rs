use waymark_convert_core::{Convert as _, TryConvert};

use crate::{ActionResultError, Converter, MissingOutcomeError};

/// Convert proto workflow arguments back into a VM ready value.
///
/// The [`proto::WorkflowArguments`] message is converted to a JSON object
/// via [`waymark_proto_message_conversions::workflow_arguments_to_json`] and then
/// into a [`waymark_vm_value_python::ReadyValue::Dict`].
///
/// Fallible since the framing-level arguments carry their values as
/// opaque encoded values that decode here.
impl TryConvert<&waymark_proto::messages::WorkflowArguments, waymark_vm_value_python::ReadyValue>
    for Converter
{
    type Error = waymark_proto_message_conversions::DecodeArgumentError;

    fn try_convert(
        value: &waymark_proto::messages::WorkflowArguments,
    ) -> Result<waymark_vm_value_python::ReadyValue, Self::Error> {
        let json = waymark_proto_message_conversions::workflow_arguments_to_json(value)?;
        let value = waymark_vm_value_convert_json::Converter::convert(json);
        Ok(value)
    }
}

/// Convert a single proto workflow argument value back into a VM ready
/// value.
///
/// This is the reverse of the
/// [`TryConvert<&ReadyValue, WorkflowArgumentValue>`] impl on this
/// converter.  The proto value is converted to JSON via
/// [`waymark_proto_python_value_conversions::workflow_argument_value_to_json`] and
/// then into a [`waymark_vm_value_python::ReadyValue`].
impl
    TryConvert<
        &waymark_proto::python_value::WorkflowArgumentValue,
        waymark_vm_value_python::ReadyValue,
    > for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        value: &waymark_proto::python_value::WorkflowArgumentValue,
    ) -> Result<waymark_vm_value_python::ReadyValue, Self::Error> {
        let json = waymark_proto_python_value_conversions::workflow_argument_value_to_json(value);
        waymark_vm_value_convert_json::Converter::try_convert(json)
    }
}

/// Convert an action result into the outcome that settles the awaiting
/// promise.
///
/// The framing carries one opaque payload; how the call completed is
/// the encoded value's own business, so it is read here as the flavor's
/// [`ActionResultValue`](waymark_proto::python_value::ActionResultValue):
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
        let result_value =
            waymark_proto_python_value_conversions::decode_action_result_value(&result.payload)
                .map_err(ActionResultError::Decode)?;

        Self::try_convert(result_value.outcome).map_err(ActionResultError::Outcome)
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
        Option<waymark_proto::python_value::action_result_value::Outcome>,
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
    > for Converter
{
    type Error = MissingOutcomeError;

    fn try_convert(
        outcome: Option<waymark_proto::python_value::action_result_value::Outcome>,
    ) -> Result<
        waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue>,
        Self::Error,
    > {
        use waymark_proto::python_value::action_result_value::Outcome;

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

/// Convert a raised exception into the VM exception it denotes.
impl
    TryConvert<
        &waymark_proto::python_value::WorkflowExceptionValue,
        waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
    > for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        exception: &waymark_proto::python_value::WorkflowExceptionValue,
    ) -> Result<
        waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
        Self::Error,
    > {
        Ok(waymark_vm_runtime_exception::Exception {
            type_id: exception.type_id.clone(),
            details: exception
                .details
                .as_deref()
                .map(Self::convert)
                .unwrap_or(waymark_vm_value_python::ReadyValue::None),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action_result(
        result_value: waymark_proto::python_value::ActionResultValue,
    ) -> waymark_proto::messages::ActionResult {
        waymark_proto::messages::ActionResult {
            payload: waymark_proto_python_value_conversions::encode_action_result_value(
                &result_value,
            ),
            ..Default::default()
        }
    }

    fn outcome(
        result: &waymark_proto::messages::ActionResult,
    ) -> waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value_python::ReadyValue> {
        Converter::try_convert(result).expect("the encoded value decodes")
    }

    #[test]
    fn returned_value_becomes_the_settled_value() {
        let returned = waymark_proto_python_value_conversions::returned_value(
            waymark_proto_python_value_conversions::json_to_workflow_argument_value(
                &serde_json::json!(42),
            ),
        );

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
            waymark_proto::python_value::WorkflowArgumentValue {
                kind: Some(
                    waymark_proto::python_value::workflow_argument_value::Kind::Exception(
                        Box::new(waymark_proto_python_value_conversions::exception_value(
                            "ValueError".to_owned(),
                            "boom".to_owned(),
                        )),
                    ),
                ),
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
            waymark_proto::python_value::WorkflowExceptionValue {
                type_id: "RetryCounterError".to_owned(),
                details: Some(Box::new(
                    waymark_proto_python_value_conversions::json_to_workflow_argument_value(
                        &serde_json::json!({ "message": "attempt 1 has not reached success" }),
                    ),
                )),
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
