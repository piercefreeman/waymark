use waymark_action_runtime_core::{ActionCallLossError, ActionCallStage};
use waymark_convert_core::{Convert, TryConvert};

use crate::Converter;

/// Convert an action-call loss into the exception that settles the
/// awaiting promise raised.
///
/// The runtime states the fact — raised
/// [`ACTION_EXECUTION_NOT_STARTED`](waymark_vm_exception_type_ids::ACTION_EXECUTION_NOT_STARTED)
/// or [`ACTION_EXECUTION_LOST`](waymark_vm_exception_type_ids::ACTION_EXECUTION_LOST)
/// by the stage the call provably reached, the details being whatever
/// the value converter renders a loss as in the flavor's own vocabulary —
/// and the program's own policy (a compiled-in retry, a user `except`, or
/// nothing) decides what the loss means.
impl<ValueConverter, Value>
    TryConvert<ActionCallLossError, waymark_vm_runtime_exception::Exception<Value>>
    for Converter<ValueConverter>
where
    ValueConverter: Convert<ActionCallLossError, Value>,
{
    type Error = core::convert::Infallible;

    fn try_convert(
        loss: ActionCallLossError,
    ) -> Result<waymark_vm_runtime_exception::Exception<Value>, Self::Error> {
        let type_id = match loss.stage {
            ActionCallStage::NotStarted => {
                waymark_vm_exception_type_ids::ACTION_EXECUTION_NOT_STARTED
            }
            ActionCallStage::Unknown => waymark_vm_exception_type_ids::ACTION_EXECUTION_LOST,
        };

        Ok(waymark_vm_runtime_exception::Exception {
            type_id: type_id.to_owned(),
            details: ValueConverter::convert(loss),
        })
    }
}

/// A provider whose completions structurally always carry an outcome
/// never produces an execution error to convert; this impl exists so
/// such providers satisfy the lowering bound.
impl<ValueConverter, Value>
    TryConvert<core::convert::Infallible, waymark_vm_runtime_exception::Exception<Value>>
    for Converter<ValueConverter>
{
    type Error = core::convert::Infallible;

    fn try_convert(
        never: core::convert::Infallible,
    ) -> Result<waymark_vm_runtime_exception::Exception<Value>, Self::Error> {
        match never {}
    }
}

#[cfg(test)]
mod tests;
