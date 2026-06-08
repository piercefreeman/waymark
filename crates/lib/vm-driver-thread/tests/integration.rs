use waymark_vm_runtime_effect::{EffectNumber, EmittedEffect};

use tokio_util::sync::CancellationToken;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_driver::Params;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_runtime_test::{
    StateId, TestEffect, TestInstruction, TestReadyValue, executable, function, runtime,
};

type TestEffector = (
    tokio::sync::mpsc::Sender<EmittedEffect<TestEffect>>,
    tokio::sync::mpsc::Receiver<PromiseSettlement<TestReadyValue, ()>>,
);

#[allow(clippy::type_complexity)]
fn effector() -> (
    TestEffector,
    tokio::sync::mpsc::Receiver<EmittedEffect<TestEffect>>,
    tokio::sync::mpsc::Sender<PromiseSettlement<TestReadyValue, ()>>,
) {
    let (effects_tx, effects_rx) = tokio::sync::mpsc::channel(1);
    let (settlements_tx, settlements_rx) = tokio::sync::mpsc::channel(1);
    ((effects_tx, settlements_rx), effects_rx, settlements_tx)
}

/// A driver that hits [`NoReadyFramesOrWaitingPromises`](waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises)
/// propagates that error through the thread handle.
#[tokio::test]
async fn driver_error_propagated_to_handle() {
    let (effector, mut effects_rx, settlements_tx) = effector();

    let handle = waymark_vm_driver_thread::spawn(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Emit("tick")]],
        )])),
        effector,
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    });

    // The driver emits the effect, we receive it.
    assert_eq!(
        effects_rx.recv().await,
        Some(EmittedEffect {
            effect: TestEffect::Message("tick"),
            number: EffectNumber(0),
        })
    );
    // Drop the settlement sender so the driver hits NoReadyFramesOrWaitingPromises.
    drop(settlements_tx);

    let Err(waymark_vm_driver_thread::Error::Driver(
        waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises,
    )) = handle.await
    else {
        panic!("expected NoReadyFramesOrWaitingPromises propagated through thread handle");
    };
}

/// The driver correctly resolves a promise and forwards the resulting effect
/// through the thread-spawning path.
#[tokio::test]
async fn resolves_promise_and_forwards_effect() {
    let (effector, mut effects_rx, settlements_tx) = effector();

    let handle = waymark_vm_driver_thread::spawn(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::EmitRegister(RegisterId(0))],
            ],
        )])),
        effector,
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    });

    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(41)),
            ack: (),
        })
        .await
        .expect("driver should accept the promise resolution");

    assert_eq!(
        effects_rx.recv().await,
        Some(EmittedEffect {
            effect: TestEffect::Value(TestReadyValue::Int(41)),
            number: EffectNumber(0),
        })
    );
    drop(settlements_tx);

    let Err(waymark_vm_driver_thread::Error::Driver(
        waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises,
    )) = handle.await
    else {
        panic!("expected NoReadyFramesOrWaitingPromises after promise resolution");
    };
}
