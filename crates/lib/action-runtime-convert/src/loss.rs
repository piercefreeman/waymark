use waymark_action_runtime_core::ActionCallLossError;
use waymark_convert_core::{Convert, TryConvert};

use crate::Converter;

/// Convert an action-call loss into the raised exception that settles the
/// awaiting promise.
///
/// The runtime states the fact - the stage the call provably reached -
/// and the flavor's value converter renders it as the raised exception of
/// its own vocabulary: for Python, `ActionExecutionNotStarted` or
/// `ActionExecutionLost`. The program's own policy (a compiled-in retry,
/// a user `except`, or nothing) then decides what the loss means.
impl<ValueConverter, RaisedException> TryConvert<ActionCallLossError, RaisedException>
    for Converter<ValueConverter>
where
    ValueConverter: Convert<ActionCallLossError, RaisedException>,
{
    type Error = core::convert::Infallible;

    fn try_convert(loss: ActionCallLossError) -> Result<RaisedException, Self::Error> {
        Ok(ValueConverter::convert(loss))
    }
}

/// A provider whose completions structurally always carry an outcome
/// never produces an execution error to convert; this impl exists so
/// such providers satisfy the lowering bound.
impl<ValueConverter, RaisedException> TryConvert<core::convert::Infallible, RaisedException>
    for Converter<ValueConverter>
{
    type Error = core::convert::Infallible;

    fn try_convert(never: core::convert::Infallible) -> Result<RaisedException, Self::Error> {
        match never {}
    }
}

#[cfg(test)]
mod tests;
