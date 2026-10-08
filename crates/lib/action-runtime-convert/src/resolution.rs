use waymark_action_runtime_core::{ActionCallLossError, ActionCallOutcome};
use waymark_convert_core::{Convert, TryConvert};
use waymark_vm_driver_core::PromiseResolution;

use crate::Converter;

/// Convert an action-call execution result into the settlement's promise
/// resolution, for a provider whose execution error is a loss.
///
/// An outcome settles directly: a value resolves the promise, an
/// exception rejects it.  An execution that produced no outcome settles
/// the promise raised, with the wrapped conversion stating how the loss
/// renders as the raised exception.
///
/// The bound is on the wrapped converter rather than on `Self` so the
/// obligation never routes back through this impl: with the raised
/// exception a bare type parameter, a `Self: Convert<_, _>` bound makes
/// this impl a candidate for itself and the trait solver overflows.
impl<ValueConverter, Value, RaisedException>
    TryConvert<
        Result<ActionCallOutcome<Value, RaisedException>, ActionCallLossError>,
        PromiseResolution<Value, RaisedException>,
    > for Converter<ValueConverter>
where
    ValueConverter: Convert<ActionCallLossError, RaisedException>,
{
    type Error = core::convert::Infallible;

    fn try_convert(
        execution_result: Result<ActionCallOutcome<Value, RaisedException>, ActionCallLossError>,
    ) -> Result<PromiseResolution<Value, RaisedException>, Self::Error> {
        Ok(settle(execution_result, Self::convert))
    }
}

/// Convert an action-call execution result into the settlement's promise
/// resolution, for a provider whose completions structurally always carry
/// an outcome.
///
/// There is no execution error to lower; the outcome settles directly.
impl<ValueConverter, Value, RaisedException>
    TryConvert<
        Result<ActionCallOutcome<Value, RaisedException>, core::convert::Infallible>,
        PromiseResolution<Value, RaisedException>,
    > for Converter<ValueConverter>
{
    type Error = core::convert::Infallible;

    fn try_convert(
        execution_result: Result<
            ActionCallOutcome<Value, RaisedException>,
            core::convert::Infallible,
        >,
    ) -> Result<PromiseResolution<Value, RaisedException>, Self::Error> {
        Ok(settle(execution_result, |never| match never {}))
    }
}

/// Settle an execution result: an outcome as it stands, an execution
/// error as the raised exception `lower` renders it as.
fn settle<Value, RaisedException, ExecutionError>(
    execution_result: Result<ActionCallOutcome<Value, RaisedException>, ExecutionError>,
    lower: impl FnOnce(ExecutionError) -> RaisedException,
) -> PromiseResolution<Value, RaisedException> {
    match execution_result {
        Ok(ActionCallOutcome::Value(value)) => PromiseResolution::Resolved(value),
        Ok(ActionCallOutcome::Exception(exception)) => PromiseResolution::Rejected(exception),
        Err(execution_error) => PromiseResolution::Rejected(lower(execution_error)),
    }
}
