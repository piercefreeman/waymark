//! `ExtCallSet::ActionCall` emits an action-call effect and queues the resumed
//! frame to observe the pending promise.

use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{RuntimeInstruction, TestEffect, TestReadyValue, new_runtime_with_args};

#[test]
fn runtime_emits_an_action_call_and_queues_the_resumed_frame() {
    let mut runtime = new_runtime_with_args(
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![
                vec![
                    ExtCallSet::ActionCall {
                        dst: RegisterId(1),
                        action_ref: 7,
                        args: vec![RegisterId(0)],
                        resume: StateId(1),
                    }
                    .into(),
                ],
                vec![RuntimeInstruction::InspectPending(RegisterId(1))],
            ],
        )]),
        vec![TestReadyValue(41)],
    );

    let emitted_effect = runtime
        .run()
        .expect("first run should emit the action call");
    let TestEffect::ExtCallSet(Effect::ActionCall {
        promise_state_id,
        action_ref,
        args,
    }) = emitted_effect.effect
    else {
        panic!("first run should emit an action call");
    };

    assert_eq!(action_ref, 7);
    assert_eq!(args, vec![41]);

    let emitted_effect = runtime
        .run()
        .expect("second run should execute the resumed frame");
    let TestEffect::PendingPromiseStateId(resumed_promise_state_id) = emitted_effect.effect else {
        panic!("second run should inspect the resumed pending promise");
    };

    assert_eq!(resumed_promise_state_id, promise_state_id);
}
