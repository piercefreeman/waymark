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

/// The recording hooks' type parameters, held as a function pointer type
/// so that they pin the effect, value and error types without owning
/// them.
type Parameters<Effect, Value, Error> = fn() -> (Effect, Value, Error);

/// Hooks that record which hook fired, in order, into a shared log.
struct Recording<Effect, Value, Error> {
    log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    _parameters: std::marker::PhantomData<Parameters<Effect, Value, Error>>,
}

impl<Effect, Value, Error> Recording<Effect, Value, Error> {
    fn new() -> (Self, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let hooks = Self {
            log: std::sync::Arc::clone(&log),
            _parameters: std::marker::PhantomData,
        };
        (hooks, log)
    }

    fn record(&self, entry: String) {
        self.log.lock().unwrap().push(entry);
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::effect_emitted::HasEffect
    for Recording<Effect, Value, Error>
{
    type Effect = Effect;
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::promise_settled::HasValue
    for Recording<Effect, Value, Error>
{
    type Value = Value;
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::vm_stopped::HasError
    for Recording<Effect, Value, Error>
{
    type Error = Error;
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::VmStarted for Recording<Effect, Value, Error> {
    fn vm_started(&self) {
        self.record("vm_started".to_owned());
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::EffectEmitted
    for Recording<Effect, Value, Error>
{
    fn effect_emitted(&self, number: waymark_vm_runtime_effect::EffectNumber, _effect: &Effect) {
        self.record(format!("effect_emitted:{number}"));
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::PromiseSettled
    for Recording<Effect, Value, Error>
{
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        _resolution: &PromiseResolution<Value>,
    ) {
        self.record(format!("promise_settled:{promise_state_id:?}"));
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::SnapshotPersisted
    for Recording<Effect, Value, Error>
{
    fn snapshot_persisted(&self, _size_in_bytes: usize) {
        self.record("snapshot_persisted".to_owned());
    }
}

impl<Effect, Value, Error> waymark_vm_driver_hooks::VmStopped for Recording<Effect, Value, Error>
where
    Error: core::fmt::Debug,
{
    fn vm_stopped(&self, error: &Error) {
        self.record(format!("vm_stopped:{error:?}"));
    }
}

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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
async fn fires_the_hooks_in_run_order() {
    let (effector, mut effects_rx, settlements_tx) = effector();
    let (hooks, log) = Recording::new();

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
        hooks,
    }));

    settlements_tx
        .send(PromiseSettlement {
            promise_state_id: PromiseStateId(0),
            resolution: PromiseResolution::Resolved(TestReadyValue::Int(41)),
            ack: (),
        })
        .await
        .expect("driver should accept the promise resolution");
    effects_rx
        .recv()
        .await
        .expect("the resumed frame emits its effect");
    drop(settlements_tx);

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::NoReadyFramesOrWaitingPromises)
    ));

    // The run's own order: started first; the settlement is observed
    // before the effect it lets the frame emit; each persisted snapshot
    // is reported after it is stored; stopped last, with the run's error.
    assert_eq!(
        *log.lock().unwrap(),
        [
            "vm_started",
            "promise_settled:PromiseStateId(0)",
            "snapshot_persisted",
            "effect_emitted:0",
            "snapshot_persisted",
            "vm_stopped:NoReadyFramesOrWaitingPromises",
        ]
    );
}

#[tokio::test]
async fn fires_vm_stopped_last_when_cancelled() {
    let (effector, _effects_rx, _settlements_tx) = effector();
    let (hooks, log) = Recording::new();
    let cancel = CancellationToken::new();

    let task = tokio::spawn(run(Params {
        runtime: runtime(executable(vec![function(
            1,
            vec![vec![TestInstruction::Suspend {
                dst: RegisterId(0),
                resume: StateId(0),
            }]],
        )])),
        effector,
        persister: (),
        codec: RmpCodec,
        cancel: cancel.clone(),
        hooks,
    }));

    // The run is parked on its promise; cancelling is the only way out.
    tokio::task::yield_now().await;
    cancel.cancel();

    assert!(matches!(
        task.await.expect("driver task should join"),
        Err(Error::Cancelled)
    ));

    let log = log.lock().unwrap();
    assert_eq!(*log, ["vm_started", "vm_stopped:Cancelled"]);
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
        hooks: waymark_vm_driver_hooks_noop::Noop::new(),
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
