use waymark_vm_runtime_effect::EmittedEffect;

use tokio_util::sync::CancellationToken;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_driver::{Error, Params, run};
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_core::RegisterId;
use waymark_vm_runtime_exception::Exception;
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_runtime_test::{
    StateId, TestEffect, TestExecutionError, TestInstruction, TestReadyValue, executable, function,
    runtime,
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

#[tokio::test]
async fn forwards_emitted_effects() {
    let (effector, mut effects_rx, settlements_tx) = effector();

    let task = tokio::spawn(run(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Emit("tick")]],
        )])),
        effector,
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    }));

    assert_eq!(
        effects_rx.recv().await,
        Some(EmittedEffect {
            effect: TestEffect::Message("tick"),
            number: waymark_vm_runtime_effect::EffectNumber(0),
        })
    );
    drop(settlements_tx);

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::NoReadyFramesOrWaitingPromises)
    ));
}

#[tokio::test]
async fn resumes_promises_and_forwards_resolved_effect() {
    let (effector, mut effects_rx, settlements_tx) = effector();

    let task = tokio::spawn(run(Params {
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
    }));

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
            number: waymark_vm_runtime_effect::EffectNumber(0),
        })
    );
    drop(settlements_tx);

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::NoReadyFramesOrWaitingPromises)
    ));
}

#[tokio::test]
async fn effect_handling_error_when_receiver_dropped() {
    let (effects_tx, effects_rx) = tokio::sync::mpsc::channel::<EmittedEffect<TestEffect>>(1);
    let (_settlements_tx, settlements_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<TestReadyValue, ()>>(1);
    drop(effects_rx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Emit("tick")]],
        )])),
        effector: (effects_tx, settlements_rx),
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    })
    .await;

    assert!(matches!(result, Err(Error::EffectHandling(_))));
}

#[tokio::test]
async fn getting_settlements_error_when_sender_dropped() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel::<EmittedEffect<TestEffect>>(1);
    let (settlements_tx, settlements_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<TestReadyValue, ()>>(1);
    drop(settlements_tx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }]],
        )])),
        effector: (effects_tx, settlements_rx),
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    })
    .await;

    assert!(matches!(result, Err(Error::GettingPromiseSettlements(()))));
}

#[tokio::test]
async fn returns_step_errors() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel::<EmittedEffect<TestEffect>>(1);
    let (_settlements_tx, settlements_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<TestReadyValue, ()>>(1);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Fail("boom")]],
        )])),
        effector: (effects_tx, settlements_rx),
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    })
    .await;

    assert!(matches!(
        result,
        Err(Error::Step(waymark_vm_runtime::step::Error::Execution(
            TestExecutionError("boom")
        )))
    ));
}

#[tokio::test]
async fn duplicate_resolutions_are_ignored() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel::<EmittedEffect<TestEffect>>(1);
    let (settlements_tx, settlements_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<TestReadyValue, ()>>(2);

    // At-least-once delivery: the same promise settled twice.  The
    // first resolution wins, the redelivery is dropped, and the VM
    // runs to completion.
    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(10)),
            ack: (),
        })
        .await
        .unwrap();
    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(11)),
            ack: (),
        })
        .await
        .unwrap();
    drop(settlements_tx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::Exit],
            ],
        )])),
        effector: (effects_tx, settlements_rx),
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    })
    .await;

    assert!(matches!(result, Err(Error::NoReadyFramesOrWaitingPromises)));
}

#[tokio::test]
async fn duplicate_rejection_after_resolution_is_ignored() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel::<EmittedEffect<TestEffect>>(1);
    let (settlements_tx, settlements_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<TestReadyValue, ()>>(2);

    // A redelivered settlement may even change flavor: a rejection
    // arriving for an already-resolved promise is dropped the same way.
    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(10)),
            ack: (),
        })
        .await
        .unwrap();
    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Rejected(Exception {
                type_id: "late".to_string(),
                details: TestReadyValue::Int(99),
            }),
            ack: (),
        })
        .await
        .unwrap();
    drop(settlements_tx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::Exit],
            ],
        )])),
        effector: (effects_tx, settlements_rx),
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    })
    .await;

    assert!(matches!(result, Err(Error::NoReadyFramesOrWaitingPromises)));
}

#[tokio::test]
async fn unknown_promise_ids_are_ignored() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel::<EmittedEffect<TestEffect>>(1);
    let (settlements_tx, settlements_rx) =
        tokio::sync::mpsc::channel::<PromiseSettlement<TestReadyValue, ()>>(2);

    // A settlement for a promise this runtime does not know — e.g. a
    // stale redelivery for a state that was garbage collected.  It is
    // dropped, and the settlement that follows still applies normally.
    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(9),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(41)),
            ack: (),
        })
        .await
        .unwrap();
    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(10)),
            ack: (),
        })
        .await
        .unwrap();
    drop(settlements_tx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::Exit],
            ],
        )])),
        effector: (effects_tx, settlements_rx),
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    })
    .await;

    assert!(matches!(result, Err(Error::NoReadyFramesOrWaitingPromises)));
}

#[tokio::test]
async fn promise_rejection_forwards_exception() {
    let (effector, mut effects_rx, settlements_tx) = effector();

    let task = tokio::spawn(run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![
                vec![TestInstruction::Suspend {
                    dst: RegisterId(0),
                    resume: StateId(1),
                }],
                vec![TestInstruction::EmitException],
            ],
        )])),
        effector,
        persister: (),
        codec: RmpCodec,
        cancel: CancellationToken::new(),
    }));

    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Rejected(Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(41),
            }),
            ack: (),
        })
        .await
        .unwrap();

    assert_eq!(
        effects_rx.recv().await,
        Some(EmittedEffect {
            effect: TestEffect::UnhandledException(Exception {
                type_id: "ValueError".to_owned(),
                details: TestReadyValue::Int(41),
            }),
            number: waymark_vm_runtime_effect::EffectNumber(0),
        })
    );
    drop(settlements_tx);

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::NoReadyFramesOrWaitingPromises)
    ));
}
