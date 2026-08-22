use waymark_action_runtime_core::ActionCallOutcome;
use waymark_convert_core::{Convert, TryConvert};

use crate::Converter;

/// Convert an action-call execution result into the settlement's promise
/// resolution.
///
/// An outcome settles directly: a value resolves the promise, an
/// exception rejects it.  An execution that produced no outcome settles
/// the promise raised, with the wrapped conversion stating how the
/// execution error renders as the raising exception.
impl<ValueConverter, Value, ExecutionError>
    TryConvert<
        Result<ActionCallOutcome<Value>, ExecutionError>,
        waymark_vm_driver_core::PromiseResolution<Value>,
    > for Converter<ValueConverter>
where
    Self: Convert<ExecutionError, waymark_vm_runtime_exception::Exception<Value>>,
{
    type Error = core::convert::Infallible;

    fn try_convert(
        execution_result: Result<ActionCallOutcome<Value>, ExecutionError>,
    ) -> Result<waymark_vm_driver_core::PromiseResolution<Value>, Self::Error> {
        Ok(match execution_result {
            Ok(ActionCallOutcome::Value(value)) => {
                waymark_vm_driver_core::PromiseResolution::Resolved(value)
            }
            Ok(ActionCallOutcome::Exception(exception)) => {
                waymark_vm_driver_core::PromiseResolution::Rejected(exception)
            }
            Err(execution_error) => {
                waymark_vm_driver_core::PromiseResolution::Rejected(Self::convert(execution_error))
            }
        })
    }
}
