//! `ExcSet::CatchException` and `ExcSet::ShouldBubble` semantics.

use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeEffect, RuntimeInstruction, TestValue, run};

#[test]
fn catch_exception_disables_bubbling_for_followup_checks() {
    let effect = run(
        2,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Int(7)),
            },
            ExcSet::CatchException { src: RegisterId(0) }.into(),
            ExcSet::ShouldBubble {
                dst: RegisterId(1),
                src: RegisterId(0),
            }
            .into(),
            RuntimeInstruction::EmitRegister(RegisterId(1)),
        ],
    )
    .expect("caught exception should no longer bubble");

    assert_eq!(effect, RuntimeEffect::Register(TestValue::Bool(false)));
}

#[test]
fn raise_reengages_bubbling_even_after_catch() {
    let effect = run(
        1,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: TestValue::exception("synthetic.failure", TestValue::Int(7)),
            },
            ExcSet::CatchException { src: RegisterId(0) }.into(),
            ExcSet::Raise { src: RegisterId(0) }.into(),
        ],
    )
    .expect("top-level raise should complete with bubbled exception");

    assert_eq!(
        effect,
        RuntimeEffect::Complete(Err(TestValue::Exception(Box::new(
            waymark_vm_runtime_exception::Exception {
                type_id: "synthetic.failure".to_owned(),
                details: TestValue::Int(7),
                bubble: true,
            },
        ))))
    );
}
