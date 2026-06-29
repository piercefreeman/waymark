//! Synchronous mix of pure and core instructions completing in one run.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_pureset::{BinaryOp, BinaryOpKind, PureSet};
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{executable, function};

use crate::support::{Instruction, TestConstValue, TestReadyValue, new_runtime};

#[test]
fn runtime_executes_pure_and_core_instructions_to_completion() {
    let executable = executable(vec![function::<Instruction>(
        3,
        vec![vec![
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
            CoreSet::Return { src: RegisterId(2) }.into(),
        ]],
    )]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("mixed fullset program should complete successfully");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestReadyValue::Int(5));
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::UnhandledException(exception)) => {
            panic!("program should not raise an exception: {exception:?}")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("program should not emit an action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("program should not emit a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    }
}
