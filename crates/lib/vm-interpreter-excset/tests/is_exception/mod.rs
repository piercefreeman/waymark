//! `ExcSet::IsException` happy paths and conversion failures.

use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_interpreter_excset::Error;
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeInstruction, TestValue, run};

#[test]
fn runtime_executes_is_exception_to_true_for_matching_exception_types() {
    let value = run(
        3,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Int(7)),
            },
            RuntimeInstruction::SetValue {
                dst: RegisterId(1),
                value: TestValue::Text("synthetic.failure"),
            },
            ExcSet::IsException {
                dst: RegisterId(2),
                src: RegisterId(0),
                exception_type_id: Some(RegisterId(1)),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit true for matching exception types");

    assert_eq!(value, TestValue::Bool(true));
}

#[test]
fn runtime_executes_is_exception_to_false_for_non_matching_exception_types() {
    let value = run(
        3,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Int(7)),
            },
            RuntimeInstruction::SetValue {
                dst: RegisterId(1),
                value: TestValue::Text("synthetic.timeout"),
            },
            ExcSet::IsException {
                dst: RegisterId(2),
                src: RegisterId(0),
                exception_type_id: Some(RegisterId(1)),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit false for non-matching exception types");

    assert_eq!(value, TestValue::Bool(false));
}

#[test]
fn runtime_executes_is_exception_to_false_for_non_exception_values() {
    let value = run(
        3,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::Text("not an exception"),
            },
            RuntimeInstruction::SetValue {
                dst: RegisterId(1),
                value: TestValue::Text("synthetic.failure"),
            },
            ExcSet::IsException {
                dst: RegisterId(2),
                src: RegisterId(0),
                exception_type_id: Some(RegisterId(1)),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit false for non-exception values");

    assert_eq!(value, TestValue::Bool(false));
}

#[test]
fn runtime_executes_is_exception_to_true_for_exception_wildcard() {
    let value = run(
        3,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Int(7)),
            },
            ExcSet::IsException {
                dst: RegisterId(2),
                src: RegisterId(0),
                exception_type_id: None,
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should treat a missing type id as a catch-all exception check");

    assert_eq!(value, TestValue::Bool(true));
}

#[test]
fn runtime_surfaces_is_exception_type_id_conversion_errors() {
    let result = run(
        3,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Int(7)),
            },
            RuntimeInstruction::SetValue {
                dst: RegisterId(1),
                value: TestValue::Int(9),
            },
            ExcSet::IsException {
                dst: RegisterId(2),
                src: RegisterId(0),
                exception_type_id: Some(RegisterId(1)),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::IsException(waymark_vm_interpreter_excset::value::NotAnExceptionTypeIdError),
        )))
    ));
}
