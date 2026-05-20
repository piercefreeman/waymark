use waymark_vm_driver::{Error, Params, run};
use waymark_vm_runtime_core::{RegisterId, ResolvePromiseError};
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_runtime_test::{
    StateId, TestEffect, TestExecutionError, TestInstruction, TestReadyValue, executable, function,
    runtime,
};

#[tokio::test]
async fn forwards_emitted_effects_to_the_effect_channel() {
    let (effects_tx, mut effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);

    let task = tokio::spawn(run(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Emit("tick")]],
        )])),
        effects_tx,
        promise_resolutions_rx,
    }));

    assert_eq!(effects_rx.recv().await, Some(TestEffect::Message("tick")));
    drop(promise_resolutions_tx);

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::PromiseResolutionReceiverClosed)
    ));
}

#[tokio::test]
async fn resumes_waiting_promises_and_forwards_the_resolved_effect() {
    let (effects_tx, mut effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);

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
        effects_tx,
        promise_resolutions_rx,
    }));

    promise_resolutions_tx
        .send((PromiseStateId(0), TestReadyValue(41)))
        .await
        .expect("driver should accept the promise resolution");

    assert_eq!(
        effects_rx.recv().await,
        Some(TestEffect::Value(TestReadyValue(41)))
    );
    drop(promise_resolutions_tx);

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::PromiseResolutionReceiverClosed)
    ));
}

#[tokio::test]
async fn returns_effect_sender_closed_when_the_effect_receiver_is_dropped() {
    let (effects_tx, effects_rx) = tokio::sync::mpsc::channel(1);
    let (_promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);
    drop(effects_rx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Emit("tick")]],
        )])),
        effects_tx,
        promise_resolutions_rx,
    })
    .await;

    assert!(matches!(result, Err(Error::EffectSenderClosed)));
}

#[tokio::test]
async fn returns_promise_resolution_receiver_closed_when_waiting_runtime_cannot_resume() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);
    drop(promise_resolutions_tx);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }]],
        )])),
        effects_tx,
        promise_resolutions_rx,
    })
    .await;

    assert!(matches!(
        result,
        Err(Error::PromiseResolutionReceiverClosed)
    ));
}

#[tokio::test]
async fn returns_step_errors_from_the_runtime() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel(1);
    let (_promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            0,
            vec![vec![TestInstruction::Fail("boom")]],
        )])),
        effects_tx,
        promise_resolutions_rx,
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
async fn returns_resolving_promise_errors_for_duplicate_resolutions() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(2);

    promise_resolutions_tx
        .send((PromiseStateId(0), TestReadyValue(10)))
        .await
        .expect("first resolution should enqueue");
    promise_resolutions_tx
        .send((PromiseStateId(0), TestReadyValue(11)))
        .await
        .expect("second resolution should enqueue");

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
        effects_tx,
        promise_resolutions_rx,
    })
    .await;

    let Err(Error::ResolvingPromise(err)) = result else {
        panic!("duplicate promise resolution should surface as a driver error");
    };

    let ResolvePromiseError::AlreadyResolved(err) = err else {
        panic!("duplicate promise resolution should report already-resolved errors");
    };

    assert_eq!(err.new_value, TestReadyValue(11));
}

#[tokio::test]
async fn returns_not_found_errors_for_unknown_promise_ids() {
    let (effects_tx, _effects_rx) = tokio::sync::mpsc::channel(1);
    let (promise_resolutions_tx, promise_resolutions_rx) = tokio::sync::mpsc::channel(1);

    promise_resolutions_tx
        .send((PromiseStateId(9), TestReadyValue(41)))
        .await
        .expect("invalid resolution should still enqueue");

    let result = run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(1),
            }]],
        )])),
        effects_tx,
        promise_resolutions_rx,
    })
    .await;

    let Err(Error::ResolvingPromise(ResolvePromiseError::PromiseStateNotFound(err))) = result
    else {
        panic!("unknown promise IDs should surface a not-found error");
    };

    assert_eq!(err.promise_state_id, PromiseStateId(9));
}
