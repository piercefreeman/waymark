//! `Length` and its error paths (including overflow and unusable values).

use waymark_vm_instructions_pureset::PureSet;
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
fn runtime_raises_a_type_error_for_an_unsupported_length_value() {
    let value = run(
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
            RuntimeInstruction::EmitPendingException,
        ],
    )
    .expect("runtime should emit the pending exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "TypeError".to_owned(),
            details: Box::new(TestValue::Text(
                "determining length is not supported for this value".to_owned()
            )),
        }
    );
}

#[test]
fn runtime_raises_an_overflow_error_for_an_unrepresentable_length_result() {
    let value = run(
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
            RuntimeInstruction::EmitPendingException,
        ],
    )
    .expect("runtime should emit the pending exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "OverflowError".to_owned(),
            details: Box::new(TestValue::Text("length result is out of bounds".to_owned())),
        }
    );
}

#[test]
fn runtime_raises_a_type_error_for_an_unusable_length_value() {
    let value = run(
        2,
        vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::Length {
                dst: RegisterId(1),
                src: RegisterId(0),
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
                "determining length is not supported for this value".to_owned()
            )),
        }
    );
}
