//! `ExtCallSet::ActionCall` suspends the runtime on an external action and
//! resumes with its resolved value.

use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_instructions_pureset::{BinaryOp, BinaryOpKind, PureSet};
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{
    Instruction, TestActionRef, TestConstValue, TestReadyValue, new_runtime_with_args,
};

#[test]
fn runtime_resumes_extcalls_and_finishes_with_pure_work() {
    let executable = executable(vec![function::<Instruction>(
        4,
        vec![
            vec![
                ExtCallSet::ActionCall {
                    dst: RegisterId(1),
                    action_ref: TestActionRef(7),
                    args: vec![RegisterId(0)],
                    resume: StateId(1),
                }
                .into(),
            ],
            vec![
                CoreSet::Await {
                    dst: RegisterId(2),
                    src: RegisterId(1),
                    resume: StateId(2),
                }
                .into(),
            ],
            vec![
                PureSet::LoadConst {
                    dst: RegisterId(3),
                    value: TestConstValue::Int(1),
                }
                .into(),
                PureSet::Binary {
                    kind: BinaryOpKind::Add,
                    op: BinaryOp {
                        dst: RegisterId(3),
                        a: RegisterId(2),
                        b: RegisterId(3),
                    },
                }
                .into(),
                CoreSet::Return { src: RegisterId(3) }.into(),
            ],
        ],
    )]);

    let mut runtime = new_runtime_with_args(executable, vec![TestReadyValue::Int(41)]);

    let effect = runtime
        .run()
        .expect("first run should emit the action call");

    let promise_state_id = match effect {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id,
            action_ref,
            args,
        }) => {
            assert_eq!(action_ref, TestActionRef(7));
            assert_eq!(args, vec![TestReadyValue::Int(41)]);
            promise_state_id
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(_)) => {
            panic!("program should suspend on the action call before completion")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("program should suspend on an extcall, not a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    };

    runtime
        .resolve_promise(promise_state_id, TestReadyValue::Int(41))
        .expect("action call promise should resolve cleanly");

    let effect = runtime
        .run()
        .expect("second run should finish after resuming the action call");

    match effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestReadyValue::Int(42));
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("resolved action call should not emit another action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("resolved extcall should not emit a sleep effect")
        }
        Effect::PureSet(effect) => match effect {},
    }
}
