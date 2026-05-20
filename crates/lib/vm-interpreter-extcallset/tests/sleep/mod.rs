//! `ExtCallSet::Sleep` emits a sleep effect and queues the resumed frame to
//! observe the pending promise.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_test::{StateId, executable, function};

use crate::support::{RuntimeInstruction, TestEffect, TestReadyValue, new_runtime_with_args};

#[test]
fn runtime_emits_a_sleep_effect_and_queues_the_resumed_frame() {
    let mut runtime = new_runtime_with_args(
        executable(vec![function::<RuntimeInstruction>(
            2,
            vec![
                vec![
                    ExtCallSet::Sleep {
                        dst: RegisterId(1),
                        duration: RegisterId(0),
                        resume: StateId(1),
                    }
                    .into(),
                ],
                vec![RuntimeInstruction::InspectPending(RegisterId(1))],
            ],
        )]),
        vec![TestReadyValue(5)],
    );

    let TestEffect::ExtCallSet(Effect::Sleep {
        promise_state_id,
        duration,
    }) = runtime
        .run()
        .expect("first run should emit the sleep effect")
    else {
        panic!("first run should emit a sleep effect");
    };

    assert_eq!(duration, NonZeroDuration::from_secs(5).unwrap());

    let TestEffect::PendingPromiseStateId(resumed_promise_state_id) = runtime
        .run()
        .expect("second run should execute the resumed frame")
    else {
        panic!("second run should inspect the resumed pending promise");
    };

    assert_eq!(resumed_promise_state_id, promise_state_id);
}
