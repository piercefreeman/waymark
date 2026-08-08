//! Behavioral tests for the Python coreset operations.

use indexmap::IndexMap;
use waymark_vm_interpreter_coreset::operations::ShouldJump as _;
use waymark_vm_interpreter_operations::Operations;
use waymark_vm_interpreter_operations_python::PythonVariation;
use waymark_vm_value::{ReadyValue, Value};

type PythonOperations = Operations<PythonVariation>;

#[test]
fn values_follow_truthiness() {
    assert!(PythonOperations::should_jump(&ReadyValue::String("x".to_owned())).unwrap());
    assert!(!PythonOperations::should_jump(&ReadyValue::String(String::new())).unwrap());
    assert!(!PythonOperations::should_jump(&ReadyValue::None).unwrap());
    assert!(PythonOperations::should_jump(&ReadyValue::Float(1.5.try_into().unwrap())).unwrap());
    assert!(!PythonOperations::should_jump(&ReadyValue::Float(0.0.try_into().unwrap())).unwrap());
    assert!(
        PythonOperations::should_jump(&ReadyValue::List(vec![Value::Ready(ReadyValue::Int(1))]))
            .unwrap()
    );
    assert!(
        PythonOperations::should_jump(&ReadyValue::Dict(IndexMap::from([(
            "key".to_owned(),
            Value::Ready(ReadyValue::Int(1))
        )])))
        .unwrap()
    );
    assert!(
        PythonOperations::should_jump(&ReadyValue::Exception(Box::new(
            waymark_vm_runtime_exception::Exception {
                type_id: "ValueError".to_owned(),
                details: Value::Ready(ReadyValue::String("boom".to_owned())),
            }
        )))
        .unwrap()
    );
}

#[test]
fn promise_level_truthiness_requires_a_ready_value() {
    assert!(PythonOperations::should_jump(&Value::Ready(ReadyValue::Int(1))).unwrap());
    assert!(!PythonOperations::should_jump(&Value::Ready(ReadyValue::Int(0))).unwrap());

    let pending = Value::Pending(waymark_vm_runtime_promise_core::PromiseStateId(4));
    assert!(matches!(
        PythonOperations::should_jump(&pending).unwrap_err(),
        waymark_vm_interpreter_operations::promise::MaybeUnresolvedError::Unresolved(error)
            if error.promise_state_id == waymark_vm_runtime_promise_core::PromiseStateId(4)
    ));
}
