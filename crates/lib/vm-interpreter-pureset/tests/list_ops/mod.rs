//! `MakeList` / `Index` happy paths and failure modes.

use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeInstruction, TestConstValue, TestValue, run};

#[test]
fn runtime_executes_make_list_to_a_terminal_effect() {
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
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(2),
                items: vec![RegisterId(0), RegisterId(1)],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit the constructed list");

    assert_eq!(
        value,
        TestValue::List(vec![TestValue::Int(2), TestValue::Text("hello".to_owned())])
    );
}

#[test]
fn runtime_executes_index_to_a_terminal_effect() {
    let value = run(
        4,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(1),
                items: vec![RegisterId(0)],
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(2),
                value: TestConstValue::Int(-1),
            }
            .into(),
            PureSet::Index {
                dst: RegisterId(3),
                object: RegisterId(1),
                index: RegisterId(2),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ],
    )
    .expect("runtime should emit the indexed result");

    assert_eq!(value, TestValue::Int(2));
}

#[test]
fn runtime_raises_a_type_error_for_an_unusable_index_operand() {
    let value = run(
        4,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Int(2),
            }
            .into(),
            PureSet::MakeList {
                dst: RegisterId(1),
                items: vec![RegisterId(0)],
            }
            .into(),
            RuntimeInstruction::SetUnusable { dst: RegisterId(2) },
            PureSet::Index {
                dst: RegisterId(3),
                object: RegisterId(1),
                index: RegisterId(2),
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
                "indexed access is not supported for these operands".to_owned()
            )),
        }
    );
}

#[test]
fn runtime_copies_unusable_make_list_items_to_a_terminal_effect() {
    let value = run(
        2,
        vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::MakeList {
                dst: RegisterId(1),
                items: vec![RegisterId(0)],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    )
    .expect("runtime should preserve unusable list items");

    assert_eq!(value, TestValue::List(vec![TestValue::Unusable]));
}
