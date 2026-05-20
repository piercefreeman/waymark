//! `Length` and its error paths (including overflow and unusable values).

use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_interpreter_pureset::Error;
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeInstruction, TestConstValue, TestValue, run};

#[test]
fn runtime_executes_length_to_a_terminal_effect() {
    let value = run(
        4,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(2),
                items: vec![RegisterId(0), RegisterId(1)],
            }
            .into(),
            PureSet::Length {
                dst: RegisterId(3),
                src: RegisterId(2),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ],
    )
    .expect("runtime should emit the list length");

    assert_eq!(value, TestValue::Int(2));
}

#[test]
fn runtime_surfaces_length_errors_from_the_pureset_interpreter() {
    let result = run(
        2,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::Length(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
        )))
    ));
}

#[test]
fn runtime_surfaces_from_length_errors_from_the_pureset_interpreter() {
    let result = run(
        2,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::OverflowLength,
            }
            .into(),
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::FromLength(
                waymark_vm_interpreter_pureset::value::FromLengthError::ResultOutOfBounds
            )
        )))
    ));
}

#[test]
fn runtime_surfaces_length_errors_from_unusable_values() {
    let result = run(
        2,
        vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::Length(waymark_vm_interpreter_pureset::value::LengthError::UnsupportedValue)
        )))
    ));
}
