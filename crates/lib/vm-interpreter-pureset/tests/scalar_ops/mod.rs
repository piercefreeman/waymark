//! Scalar-oriented pureset operations: `LoadConst`, `Copy`, `Binary::Add`,
//! `Unary::Not`, and their failure modes.

use waymark_vm_instructions_pureset::{BinaryOp, BinaryOpKind, PureSet, UnaryOp, UnaryOpKind};
use waymark_vm_interpreter_pureset::{BinaryOperandPosition, Error};
use waymark_vm_runtime::RunError;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeInstruction, TestConstValue, TestValue, run};

#[test]
fn runtime_executes_load_const_and_add_to_a_terminal_effect() {
    let value = run(
        3,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Binary {
                kind: BinaryOpKind::Add,
                op: BinaryOp {
                    dst: RegisterId(2),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit the computed pure result");

    assert_eq!(value, TestValue::Int(5));
}

#[test]
fn runtime_executes_copy_between_registers() {
    let value = run(
        2,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::Copy {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    )
    .expect("runtime should emit the copied pure result");

    assert_eq!(value, TestValue::Int(9));
}

#[test]
fn runtime_executes_unary_not_to_a_terminal_effect() {
    let value = run(
        2,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(0),
            }
            .into(),
            PureSet::Unary {
                kind: UnaryOpKind::Not,
                op: UnaryOp {
                    dst: RegisterId(1),
                    src: RegisterId(0),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    )
    .expect("runtime should emit the unary-not result");

    assert_eq!(value, TestValue::Bool(true));
}

#[test]
fn runtime_converts_non_numeric_constants_through_the_spec_type() {
    let value = run(
        1,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(0)),
        ],
    )
    .expect("runtime should emit the converted constant");

    assert_eq!(value, TestValue::Text("hello"));
}

#[test]
fn runtime_surfaces_add_errors_from_the_pureset_interpreter() {
    let result = run(
        2,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("left"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Binary {
                kind: BinaryOpKind::Add,
                op: BinaryOp {
                    dst: RegisterId(0),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(0)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::BinaryOperation {
                operation: BinaryOpKind::Add,
                source:
                    waymark_vm_interpreter_pureset::value::BinaryOperationError::UnsupportedOperation {
                        operation: BinaryOpKind::Add,
                    },
            }
        )))
    ));
}

#[test]
fn runtime_surfaces_unusable_add_operand_errors_from_unusable_values() {
    let result = run(
        2,
        vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::Binary {
                kind: BinaryOpKind::Add,
                op: BinaryOp {
                    dst: RegisterId(0),
                    a: RegisterId(0),
                    b: RegisterId(1),
                },
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(0)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableBinaryOperand {
                operation: BinaryOpKind::Add,
                operand_pos: BinaryOperandPosition::First,
                source: waymark_vm_interpreter_pureset::value::AsScalarError::NotAScalar,
            }
        )))
    ));
}
