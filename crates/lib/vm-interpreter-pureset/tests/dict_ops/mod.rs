//! `MakeDict` / `Dot` happy paths and failure modes.

use std::collections::BTreeMap;

use waymark_vm_instructions_pureset::{DictEntry, PureSet};
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeInstruction, TestConstValue, TestValue, run};

#[test]
fn runtime_executes_make_dict_to_a_terminal_effect() {
    let value = run(
        4,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("key"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Text("hello"),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    )
    .expect("runtime should emit the constructed dict");

    assert_eq!(
        value,
        TestValue::Dict(BTreeMap::from([(
            "key".to_owned(),
            TestValue::Text("hello".to_owned()),
        )]))
    );
}

#[test]
fn runtime_executes_dot_to_a_terminal_effect() {
    let value = run(
        4,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("field"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            PureSet::Dot {
                dst: RegisterId(3),
                object: RegisterId(2),
                attribute: "field".to_owned(),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ],
    )
    .expect("runtime should emit the dotted result");

    assert_eq!(value, TestValue::Int(9));
}

#[test]
fn runtime_raises_a_type_error_for_an_unsupported_dict_key_type() {
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
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
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
                "dict keys of this type are not supported".to_owned()
            )),
        }
    );
}

#[test]
fn runtime_raises_an_attribute_error_for_a_missing_dot_attribute() {
    let value = run(
        4,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("present"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            PureSet::Dot {
                dst: RegisterId(3),
                object: RegisterId(2),
                attribute: "missing".to_owned(),
            }
            .into(),
            RuntimeInstruction::EmitPendingException,
        ],
    )
    .expect("runtime should emit the pending exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "AttributeError".to_owned(),
            details: Box::new(TestValue::Text("attribute is missing".to_owned())),
        }
    );
}

#[test]
fn runtime_raises_a_key_error_for_a_missing_dict_key() {
    let value = run(
        5,
        vec![
            PureSet::LoadConst {
                dst: RegisterId(0),
                value: TestConstValue::Text("present"),
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(9),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
            }
            .into(),
            PureSet::LoadConst {
                dst: RegisterId(3),
                value: TestConstValue::Text("missing"),
            }
            .into(),
            PureSet::Index {
                dst: RegisterId(4),
                object: RegisterId(2),
                index: RegisterId(3),
            }
            .into(),
            RuntimeInstruction::EmitPendingException,
        ],
    )
    .expect("runtime should emit the pending exception");

    assert_eq!(
        value,
        TestValue::Exception {
            type_id: "KeyError".to_owned(),
            details: Box::new(TestValue::Text("key is missing".to_owned())),
        }
    );
}

#[test]
fn runtime_raises_a_type_error_for_an_unusable_dict_key() {
    let value = run(
        3,
        vec![
            RuntimeInstruction::SetUnusable { dst: RegisterId(0) },
            PureSet::LoadConst {
                dst: RegisterId(1),
                value: TestConstValue::Int(3),
            }
            .into(),
            PureSet::MakeDict {
                dst: RegisterId(2),
                entries: vec![DictEntry {
                    key: RegisterId(0),
                    value: RegisterId(1),
                }],
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
                "dict keys of this type are not supported".to_owned()
            )),
        }
    );
}
