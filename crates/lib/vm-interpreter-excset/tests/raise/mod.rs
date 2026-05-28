//! `ExcSet::Raise` completes with an exception when it is uncaught.

use waymark_vm_instructions_excset::ExcSet;
use waymark_vm_runtime_core::RegisterId;

use crate::support::{RuntimeEffect, RuntimeInstruction, TestValue, run};

#[test]
fn runtime_completes_with_exception_when_raise_has_no_handler() {
    let exception = TestValue::exception("synthetic.failure", TestValue::Int(7));

    let effect = run(
        1,
        vec![
            RuntimeInstruction::SetValue {
                dst: RegisterId(0),
                value: exception.clone(),
            },
            ExcSet::Raise { src: RegisterId(0) }.into(),
        ],
    )
    .expect("top-level raise should complete with an exception payload");

    assert_eq!(effect, RuntimeEffect::Complete(Err(exception)));
}
