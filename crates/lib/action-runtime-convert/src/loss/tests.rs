use core::convert::Infallible;

use waymark_action_runtime_core::{ActionCallLossError, ActionCallStage};
use waymark_convert_core::{Convert, TryConvert};

use crate::Converter;

/// A value converter whose rendering of a loss is recognisable.
struct RendersLossAsMarker;

impl TryConvert<ActionCallLossError, &'static str> for RendersLossAsMarker {
    type Error = Infallible;

    fn try_convert(_loss: ActionCallLossError) -> Result<&'static str, Self::Error> {
        Ok("the flavor's rendering of the loss")
    }
}

fn raised(stage: ActionCallStage) -> waymark_vm_runtime_exception::Exception<&'static str> {
    Converter::<RendersLossAsMarker>::convert(ActionCallLossError { stage })
}

#[test]
fn a_loss_that_never_started_raises_action_execution_not_started() {
    let exception = raised(ActionCallStage::NotStarted);

    assert_eq!(
        exception.type_id,
        waymark_vm_exception_type_ids::ACTION_EXECUTION_NOT_STARTED
    );
    assert_eq!(exception.details, "the flavor's rendering of the loss");
}

#[test]
fn a_loss_of_unknown_stage_raises_action_execution_lost() {
    let exception = raised(ActionCallStage::Unknown);

    assert_eq!(
        exception.type_id,
        waymark_vm_exception_type_ids::ACTION_EXECUTION_LOST
    );
    assert_eq!(exception.details, "the flavor's rendering of the loss");
}
