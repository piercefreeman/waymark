use waymark_action_runtime_core::{ActionCallLossError, ActionCallStage};
use waymark_convert_core::TryConvert;

use crate::Converter;

/// Convert an action-call loss into the exception that settles the
/// awaiting promise raised.
///
/// The runtime states the fact — raised
/// [`EXECUTION_LOST`](waymark_vm_exception_type_ids::EXECUTION_LOST),
/// the details carrying the stage the call provably reached — and the
/// program's own policy (a compiled-in retry, a user `except`, or
/// nothing) decides what the loss means.
impl
    TryConvert<
        ActionCallLossError,
        waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
    > for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        loss: ActionCallLossError,
    ) -> Result<
        waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue>,
        Self::Error,
    > {
        let stage = match loss.stage {
            ActionCallStage::NotStarted => "not_started",
            ActionCallStage::Unknown => "unknown",
        };

        let details = waymark_vm_value_python::ReadyValue::Dict(
            [(
                "stage".to_owned(),
                waymark_vm_value_python::Value::Ready(waymark_vm_value_python::ReadyValue::String(
                    stage.to_owned(),
                )),
            )]
            .into_iter()
            .collect(),
        );

        Ok(waymark_vm_runtime_exception::Exception {
            type_id: waymark_vm_exception_type_ids::EXECUTION_LOST.to_owned(),
            details,
        })
    }
}

/// A provider whose completions structurally always carry an outcome
/// never produces an execution error to convert; this impl exists so
/// such providers satisfy the lowering bound.
impl<Value> TryConvert<core::convert::Infallible, waymark_vm_runtime_exception::Exception<Value>>
    for Converter
{
    type Error = core::convert::Infallible;

    fn try_convert(
        never: core::convert::Infallible,
    ) -> Result<waymark_vm_runtime_exception::Exception<Value>, Self::Error> {
        match never {}
    }
}
