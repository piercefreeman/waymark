//! `MakeException` happy path and failure modes.

use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_interpreter_pureset::Error;
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeInstruction, TestConstValue, TestValue, run};

#[test]
fn runtime_executes_make_exception_to_a_terminal_effect() {
    let value = run(
        3,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("ValueError"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(41),
            }
            .into(),
            PureSet::MakeException {
                dst: RegisterId(2),
                type_id: RegisterId(0),
                details: RegisterId(1),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit the constructed exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "ValueError".to_owned(),
            details: Box::new(TestValue::Int(41)),
        }
    );
}

#[test]
fn runtime_surfaces_non_string_type_id_errors_from_the_pureset_interpreter() {
    let result = run(
        3,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(41),
            }
            .into(),
            PureSet::MakeException {
                dst: RegisterId(2),
                type_id: RegisterId(0),
                details: RegisterId(1),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableExceptionTypeId {
                source:
                    waymark_vm_interpreter_pureset::operations::AsExceptionTypeIdError::UnsupportedTypeIdType,
            }
        )))
    ));
}

#[test]
fn runtime_surfaces_missing_make_exception_registers() {
    let result = run(
        3,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(41),
            }
            .into(),
            PureSet::MakeException {
                dst: RegisterId(2),
                type_id: RegisterId(0),
                details: RegisterId(1),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::MissingExceptionTypeId {
                register: RegisterId(0),
            }
        )))
    ));

    let result = run(
        3,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("ValueError"),
            }
            .into(),
            PureSet::MakeException {
                dst: RegisterId(2),
                type_id: RegisterId(0),
                details: RegisterId(1),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::MissingExceptionDetails {
                register: RegisterId(1),
            }
        )))
    ));
}
