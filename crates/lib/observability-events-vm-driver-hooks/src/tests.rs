use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::NESlice;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_observability_events_core::Event;
use waymark_observability_events_payload::Payload;
use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_driver_hooks::{
    EffectEmitted as _, PromiseSettled as _, SnapshotPersisted as _, VmStarted as _, VmStopped as _,
};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::Hooks;

/// The hooks over a full-instruction-set run with `u8` values whose
/// collaborators cannot fail.
type TestHooks = Hooks<
    waymark_observability_events_vm_driver_hooks_fullset::FullSetEffectSummarizer<u8>,
    u8,
    waymark_vm_driver::Error<(), (), (), (), ()>,
>;

/// One flushed event: its position in the node's stream and its payload
/// as the store would see it.
type Seen = (u64, serde_json::Value);

struct RecordingFlusher {
    seen: Arc<std::sync::Mutex<Vec<Seen>>>,
}

impl waymark_lossy_batcher::Flusher<Event<waymark_ids::NodeId, Payload>> for RecordingFlusher {
    type Error = String;

    async fn flush(
        &self,
        batch: NESlice<'_, Event<waymark_ids::NodeId, Payload>>,
    ) -> Result<(), String> {
        self.seen.lock().unwrap().extend(batch.iter().map(|event| {
            (
                event.node_sequence.get(),
                serde_json::to_value(&event.payload).expect("serialize"),
            )
        }));
        Ok(())
    }
}

/// An emitter recording into `seen`, with the task and the sender whose
/// drop shuts the batcher down — which flushes what it holds.
fn recording_emitter(
    seen: &Arc<std::sync::Mutex<Vec<Seen>>>,
) -> (
    Arc<super::Emitter>,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let policy = waymark_lossy_batcher::Policy {
        buffers: NonZeroUsize::new(2).expect("non-zero"),
        max_batch: NonZeroUsize::new(64).expect("non-zero"),
        max_delay: NonZeroDuration::new(Duration::from_secs(60)).expect("non-zero"),
        flushers: NonZeroUsize::new(1).expect("non-zero"),
    }
    .validate()
    .expect("policy is valid");
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (batcher, task) = waymark_lossy_batcher::lossy_batcher(
        "test",
        policy,
        RecordingFlusher {
            seen: Arc::clone(seen),
        },
        async move {
            let _ = shutdown_rx.await;
        },
    );
    let emitter = waymark_observability_events_emitter::Emitter::new(
        waymark_ids::NodeId::new_uuid_v4(),
        batcher,
    );
    (Arc::new(emitter), tokio::spawn(task), shutdown_tx)
}

async fn flushed(
    seen: Arc<std::sync::Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
) -> Vec<Seen> {
    drop(shutdown_tx);
    task.await.expect("batcher task");

    seen.lock().unwrap().clone()
}

fn action_call() -> <TestHooks as waymark_vm_driver_hooks::effect_emitted::HasEffect>::Effect {
    waymark_vm_interpreter_fullset::Effect::ExtCallSet(
        waymark_vm_interpreter_extcallset::Effect::ActionCall {
            promise_state_id: PromiseStateId(3),
            action_ref: waymark_action_core::ActionRef {
                action_name: "fetch".to_owned(),
                module_name: Some("app.tasks".to_owned()),
                call_args: vec!["url".to_owned()],
            },
            args: vec![7],
        },
    )
}

/// A flushed payload, checked to be a `vm_driver` event.
fn vm_driver(seen: &Seen) -> &serde_json::Value {
    assert_eq!(seen.1["source"], "vm_driver", "{}", seen.1);
    &seen.1
}

#[tokio::test]
async fn every_hook_becomes_one_summarized_event_in_run_order() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (emitter, task, shutdown_tx) = recording_emitter(&seen);
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let hooks = TestHooks::new(vm_id, emitter);

    hooks.vm_started();
    hooks.effect_emitted(EffectNumber(0), &action_call());
    hooks.promise_settled(PromiseStateId(3), &PromiseResolution::Resolved(200));
    hooks.snapshot_persisted(4096);
    hooks.effect_emitted(
        EffectNumber(1),
        &waymark_vm_interpreter_fullset::Effect::ExtCallSet(
            waymark_vm_interpreter_extcallset::Effect::Sleep {
                promise_state_id: PromiseStateId(4),
                duration: NonZeroDuration::new(Duration::from_millis(1500)).expect("non-zero"),
                skip_allowed: false,
            },
        ),
    );
    hooks.promise_settled(
        PromiseStateId(4),
        &PromiseResolution::Rejected(waymark_vm_runtime_exception::Exception {
            type_id: "TimeoutError".to_owned(),
            details: 1,
        }),
    );
    hooks.effect_emitted(
        EffectNumber(2),
        &waymark_vm_interpreter_fullset::Effect::CoreSet(
            waymark_vm_interpreter_coreset::Effect::UnhandledException(
                waymark_vm_runtime_exception::Exception {
                    type_id: "ValueError".to_owned(),
                    details: 2,
                },
            ),
        ),
    );
    hooks.vm_stopped(&waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises);

    let seen = flushed(seen, task, shutdown_tx).await;

    let hooks_seen: Vec<_> = seen
        .iter()
        .map(|event| {
            let payload = vm_driver(event);
            assert_eq!(payload["vm_id"], serde_json::json!(vm_id));
            (
                payload["run_sequence"].clone(),
                payload["observation"].clone(),
            )
        })
        .collect();
    assert_eq!(
        hooks_seen,
        [
            (
                serde_json::json!(0),
                serde_json::json!({ "kind": "vm_started" })
            ),
            (
                serde_json::json!(1),
                serde_json::json!({ "kind": "effect_emitted", "effect_number": 0, "effect": {
                    "kind": "action_call", "promise_state_id": 3, "action_name": "fetch", "module_name": "app.tasks"
                } }),
            ),
            (
                serde_json::json!(2),
                serde_json::json!({ "kind": "promise_settled", "promise_state_id": 3, "settlement": { "kind": "resolved" } }),
            ),
            (
                serde_json::json!(3),
                serde_json::json!({ "kind": "snapshot_persisted", "size_in_bytes": 4096 }),
            ),
            (
                serde_json::json!(4),
                serde_json::json!({ "kind": "effect_emitted", "effect_number": 1, "effect": {
                    "kind": "sleep", "promise_state_id": 4, "duration": { "secs": 1, "nanos": 500_000_000 }, "skip_allowed": false
                } }),
            ),
            (
                serde_json::json!(5),
                serde_json::json!({ "kind": "promise_settled", "promise_state_id": 4, "settlement": {
                    "kind": "rejected", "exception_type": "TimeoutError"
                } }),
            ),
            (
                serde_json::json!(6),
                serde_json::json!({ "kind": "effect_emitted", "effect_number": 2, "effect": {
                    "kind": "unhandled_exception", "exception_type": "ValueError"
                } }),
            ),
            (
                serde_json::json!(7),
                serde_json::json!({ "kind": "vm_stopped", "reason": { "kind": "no_ready_frames_or_waiting_promises" } }),
            ),
        ]
    );

    let positions: Vec<_> = seen.iter().map(|event| event.0).collect();
    assert_eq!(positions, (0..8).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_failed_run_renders_the_collaborator_error_once() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (emitter, task, shutdown_tx) = recording_emitter(&seen);
    let hooks = Hooks::<
        waymark_observability_events_vm_driver_hooks_fullset::FullSetEffectSummarizer<u8>,
        u8,
        waymark_vm_driver::Error<(), (), &'static str, (), ()>,
    >::new(waymark_ids::InstanceId::new_uuid_v4(), emitter);

    hooks.vm_stopped(&waymark_vm_driver::Error::SnapshotPersistence("disk full"));

    let seen = flushed(seen, task, shutdown_tx).await;

    assert_eq!(
        vm_driver(&seen[0])["observation"],
        serde_json::json!({ "kind": "vm_stopped", "reason": {
            "kind": "snapshot_persistence", "error": "\"disk full\""
        } })
    );
}

#[tokio::test]
async fn two_vms_share_the_node_stream_with_their_own_run_positions() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (emitter, task, shutdown_tx) = recording_emitter(&seen);
    let first_vm = waymark_ids::InstanceId::new_uuid_v4();
    let second_vm = waymark_ids::InstanceId::new_uuid_v4();
    let first = TestHooks::new(first_vm, Arc::clone(&emitter));
    let second = TestHooks::new(second_vm, emitter);

    first.vm_started();
    second.vm_started();
    first.snapshot_persisted(1);
    second.snapshot_persisted(2);
    second.vm_stopped(&waymark_vm_driver::Error::Cancelled);
    first.snapshot_persisted(3);

    let seen = flushed(seen, task, shutdown_tx).await;

    let stream: Vec<_> = seen
        .iter()
        .map(|event| {
            let payload = vm_driver(event);
            (
                event.0,
                payload["vm_id"] == serde_json::json!(first_vm),
                payload["run_sequence"].as_u64().expect("a position"),
            )
        })
        .collect();
    assert_eq!(
        stream,
        [
            (0, true, 0),
            (1, false, 0),
            (2, true, 1),
            (3, false, 1),
            (4, false, 2),
            (5, true, 2),
        ]
    );
}
