use core::convert::Infallible;

use waymark_action_runtime_core::{ActionCallLossError, ActionCallStage};
use waymark_convert_core::{Convert, TryConvert};

use crate::Converter;

/// A value converter whose rendering of a loss is recognisable.
struct RendersLossByStage;

impl TryConvert<ActionCallLossError, &'static str> for RendersLossByStage {
    type Error = Infallible;

    fn try_convert(loss: ActionCallLossError) -> Result<&'static str, Self::Error> {
        Ok(match loss.stage {
            ActionCallStage::NotStarted => "the flavor's not-started exception",
            ActionCallStage::Unknown => "the flavor's lost exception",
        })
    }
}

fn raised(stage: ActionCallStage) -> &'static str {
    Converter::<RendersLossByStage>::convert(ActionCallLossError { stage })
}

#[test]
fn a_loss_is_rendered_by_the_flavor_from_its_stage() {
    assert_eq!(
        raised(ActionCallStage::NotStarted),
        "the flavor's not-started exception"
    );
    assert_eq!(
        raised(ActionCallStage::Unknown),
        "the flavor's lost exception"
    );
}
