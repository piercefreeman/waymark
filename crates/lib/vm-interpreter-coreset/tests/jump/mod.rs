//! Unconditional and conditional jumps before returning.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_interpreter_coreset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{Instruction, TestReadyValue, new_runtime_with_args};

#[test]
fn runtime_follows_jump_and_jump_if_before_returning() {
    let executable = executable(vec![function::<Instruction>(
        2,
        vec![
            vec![CoreSet::JumpIf {
                target_state: StateId(1),
                cond: RegisterId(0),
            }],
            vec![CoreSet::Jump {
                target_state: StateId(2),
            }],
            vec![CoreSet::Return { src: RegisterId(1) }],
        ],
    )]);

    let mut runtime = new_runtime_with_args(
        executable,
        vec![TestReadyValue::Int(1), TestReadyValue::Int(9)],
    );

    let emitted_effect = runtime.run().expect("jump program should complete");

    match emitted_effect.effect {
        Effect::Complete(value) => assert_eq!(value, TestReadyValue::Int(9)),
        Effect::UnhandledException(exception) => {
            panic!("jump program should not raise an exception: {exception:?}")
        }
    }
}
