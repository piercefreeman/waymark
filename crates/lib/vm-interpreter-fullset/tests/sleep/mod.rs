//! `ExtCallSet::Sleep` suspends the runtime and resumes after the promise
//! resolves.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_interpreter_fullset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{Instruction, TestConstValue, TestReadyValue, new_runtime};

#[test]
fn runtime_resumes_sleep_effects_and_finishes_with_pure_work() {
    let executable = executable(vec![function::<Instruction>(
        4,
        vec![
            vec![
                PureSet::LoadConst {
                    dst: RegisterId(0),
                    value: TestConstValue::Int(2),
                }
                .into(),
                ExtCallSet::Sleep {
                    dst: RegisterId(1),
                    duration: RegisterId(0),
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
                    value: TestConstValue::Int(7),
                }
                .into(),
                CoreSet::Return { src: RegisterId(3) }.into(),
            ],
        ],
    )]);

    let mut runtime = new_runtime(executable);

    let emitted_effect = runtime
        .run()
        .expect("first run should emit the sleep effect");

    let promise_state_id = match emitted_effect.effect {
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep {
            promise_state_id,
            duration,
        }) => {
            assert_eq!(duration, NonZeroDuration::from_secs(2).unwrap());
            promise_state_id
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(_)) => {
            panic!("program should suspend on sleep before completion")
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::UnhandledException(exception)) => {
            panic!("program should not raise an exception before suspension: {exception:?}")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("program should emit a sleep effect, not an action call")
        }
        Effect::PureSet(effect) => match effect {},
    };

    runtime
        .resolve_promise(promise_state_id, TestReadyValue::Int(0))
        .expect("sleep promise should resolve cleanly");

    let emitted_effect = runtime
        .run()
        .expect("second run should finish after resuming sleep");

    match emitted_effect.effect {
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::Complete(value)) => {
            assert_eq!(value, TestReadyValue::Int(7));
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::ActionCall { .. }) => {
            panic!("resolved sleep should not emit an action call")
        }
        Effect::ExtCallSet(waymark_vm_interpreter_extcallset::Effect::Sleep { .. }) => {
            panic!("resolved sleep should not emit another sleep effect")
        }
        Effect::CoreSet(waymark_vm_interpreter_coreset::Effect::UnhandledException(exception)) => {
            panic!("resolved sleep should not raise an exception: {exception:?}")
        }
        Effect::PureSet(effect) => match effect {},
    }
}
