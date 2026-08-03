//! Scalar-oriented pureset operations: `LoadConst`, `Copy`, `Binary::Add`,
//! `Unary::Not`, and their failure modes.

use waymark_vm_instructions_pureset::{BinaryOp, BinaryOpKind, PureSet, UnaryOp, UnaryOpKind};
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

    assert_eq!(value, TestValue::Text("hello".to_owned()));
}

#[test]
fn runtime_raises_a_type_error_for_an_unsupported_add() {
    let value = run(
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
            RuntimeInstruction::EmitPendingException,
        ],
    )
    .expect("runtime should emit the pending exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "TypeError".to_owned(),
            details: Box::new(TestValue::Text(
                "+ is not supported for these operands".to_owned()
            )),
        }
    );
}

#[test]
fn runtime_raises_a_type_error_for_an_unusable_add_operand() {
    let value = run(
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
            RuntimeInstruction::EmitPendingException,
        ],
    )
    .expect("runtime should emit the pending exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "TypeError".to_owned(),
            details: Box::new(TestValue::Text("not a scalar".to_owned())),
        }
    );
}
