//! Behavioral tests for the provided extcallset operations
//! implementations over the concrete `vm-value` shape.

use waymark_vm_interpreter_extcallset::operations::{
    CaptureActionCallArgument as _, SleepDuration as _,
};
use waymark_vm_interpreter_operations::{
    extcallset::ready::SleepDurationError, promise::MaybeUnresolvedError,
};
use waymark_vm_value::{ReadyValue, Value};

/// A local variation marker: the provided operations implementations are
/// blankets over every variation, so any marker selects them.
enum TestVariation {}

type TestOperations = waymark_vm_interpreter_operations::Operations<TestVariation>;

#[test]
fn sleep_durations_follow_runtime_semantics() {
    assert_eq!(
        TestOperations::to_sleep_duration(&ReadyValue::Int(5))
            .unwrap()
            .get(),
        std::time::Duration::from_secs(5)
    );
    assert_eq!(
        TestOperations::to_sleep_duration(&ReadyValue::Float(0.5.try_into().unwrap()))
            .unwrap()
            .get(),
        std::time::Duration::from_millis(500)
    );
    assert!(matches!(
        TestOperations::to_sleep_duration(&ReadyValue::Int(0)).unwrap_err(),
        SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        TestOperations::to_sleep_duration(&ReadyValue::Int(-1)).unwrap_err(),
        SleepDurationError::Negative
    ));
    assert!(matches!(
        TestOperations::to_sleep_duration(&ReadyValue::Float(0.0.try_into().unwrap())).unwrap_err(),
        SleepDurationError::Zero(_)
    ));
    assert!(matches!(
        TestOperations::to_sleep_duration(&ReadyValue::Float((-0.25).try_into().unwrap()))
            .unwrap_err(),
        SleepDurationError::FloatConversion(_)
    ));
    assert_eq!(
        TestOperations::to_sleep_duration(&ReadyValue::Bool(true)).unwrap_err(),
        SleepDurationError::UnsupportedValue
    );
}

#[test]
fn promise_level_operations_require_a_ready_value() {
    let ready = Value::Ready(ReadyValue::Int(5));
    assert_eq!(
        TestOperations::to_sleep_duration(&ready).unwrap().get(),
        std::time::Duration::from_secs(5)
    );
    assert!(matches!(
        TestOperations::to_sleep_duration(&ready.clone()).map(|_| ()),
        Ok(())
    ));

    let pending = Value::Pending(waymark_vm_runtime_promise_core::PromiseStateId(3));
    assert!(matches!(
        TestOperations::to_sleep_duration(&pending).unwrap_err(),
        MaybeUnresolvedError::Unresolved(error)
            if error.promise_state_id == waymark_vm_runtime_promise_core::PromiseStateId(3)
    ));
    assert!(matches!(
        TestOperations::to_sleep_duration(&Value::Ready(ReadyValue::None)).unwrap_err(),
        MaybeUnresolvedError::Ready(SleepDurationError::UnsupportedValue)
    ));
    assert!(matches!(
        TestOperations::capture_action_call_argument(&pending).unwrap_err(),
        MaybeUnresolvedError::Unresolved(_)
    ));
}

#[test]
fn action_call_arguments_are_captured_by_clone() {
    let value = ReadyValue::Exception(Box::new(waymark_vm_runtime_exception::Exception {
        type_id: "ValueError".to_owned(),
        details: Value::Ready(ReadyValue::String("boom".to_owned())),
    }));
    assert_eq!(
        TestOperations::capture_action_call_argument(&value).unwrap(),
        value.clone()
    );

    assert_eq!(
        TestOperations::capture_action_call_argument(&Value::Ready(ReadyValue::Int(7))).unwrap(),
        ReadyValue::Int(7)
    );
}
