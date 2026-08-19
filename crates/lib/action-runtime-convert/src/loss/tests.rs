use waymark_action_runtime_core::{ActionCallLossError, ActionCallStage};
use waymark_convert_core::Convert;

use crate::Converter;

fn raised(
    stage: ActionCallStage,
) -> waymark_vm_runtime_exception::Exception<waymark_vm_value_python::ReadyValue> {
    Converter::convert(ActionCallLossError { stage })
}

#[test]
fn a_loss_that_never_started_raises_action_execution_not_started() {
    let exception = raised(ActionCallStage::NotStarted);

    assert_eq!(
        exception.type_id,
        waymark_vm_exception_type_ids::ACTION_EXECUTION_NOT_STARTED
    );
    assert_eq!(exception.details, waymark_vm_value_python::ReadyValue::None);
}

#[test]
fn a_loss_of_unknown_stage_raises_action_execution_lost() {
    let exception = raised(ActionCallStage::Unknown);

    assert_eq!(
        exception.type_id,
        waymark_vm_exception_type_ids::ACTION_EXECUTION_LOST
    );
    assert_eq!(exception.details, waymark_vm_value_python::ReadyValue::None);
}
