//! `ExcSet::ExceptionDetails` happy path and failure mode.

use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_interpreter_excset::Error;
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeEffect, RuntimeInstruction, TestValue, run};

#[test]
fn runtime_executes_exception_details_to_a_terminal_effect() {
    let value = run(
        2,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Text("payload")),
            },
            ExcSet::ExceptionDetails {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    )
    .expect("runtime should emit the captured exception details");

    assert_eq!(value, RuntimeEffect::Register(TestValue::Text("payload")));
}

#[test]
fn runtime_surfaces_exception_details_errors_for_non_exception_values() {
    let result = run(
        2,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::Int(4),
            },
            ExcSet::ExceptionDetails {
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
            Error::ExceptionDetails(waymark_vm_runtime_exception::NotAnExceptionError),
        )))
    ));
}
