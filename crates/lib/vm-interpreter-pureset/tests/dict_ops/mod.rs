//! `MakeDict` / `Dot` happy paths and failure modes.

use std::collections::BTreeMap;

use waymark_vm_instructions_pureset::{DictEntry, PureSet};
use waymark_vm_interpreter_pureset::Error;
use waymark_vm_runtime::RunError;
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
            TestValue::Text("hello"),
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
fn runtime_surfaces_make_dict_key_type_errors_from_the_pureset_interpreter() {
    let result = run(
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
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableDictKey {
                entry_pos,
                source: waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType,
            }
        ))) if entry_pos == 0
    ));
}

#[test]
fn runtime_surfaces_missing_dot_attribute_errors_from_the_pureset_interpreter() {
    let result = run(
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
            RuntimeInstruction::EmitRegister(RegisterId(3)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::DotOperation {
                attribute,
                source:
                    waymark_vm_interpreter_pureset::value::DotOperationError::MissingAttribute,
            }
        ))) if attribute == "missing"
    ));
}

#[test]
fn runtime_surfaces_unusable_make_dict_key_errors_from_unusable_values() {
    let result = run(
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
            RuntimeInstruction::EmitRegister(RegisterId(2)),
        ],
    );

    assert!(matches!(
        result,
        Err(RunError::Step(waymark_vm_runtime::step::Error::Execution(
            Error::UnusableDictKey {
                entry_pos,
                source: waymark_vm_interpreter_pureset::value::AsDictKeyError::UnsupportedKeyType,
            }
        ))) if entry_pos == 0
    ));
}
