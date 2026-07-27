use std::sync::{Arc, Mutex};

use chrono::{Duration, Utc};
use waymark_state_manager_core::Factory as _;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::reconciling_factory::{Error, ReconcileVmError};
use crate::renewal::HeldLock;
use crate::test_support::{
    CapturingRequester, FailingRequester, MockBackend, MockRow, TestKey, action_ref,
};
use crate::{ActionCallRequestPayload, ReconcilingFactory};

const LOCK_TIME_TO_LIVE: std::time::Duration = std::time::Duration::from_secs(60);

/// The inner factory: records production events for ordering assertions.
struct RecordingFactory {
    events: Arc<Mutex<Vec<String>>>,
}

impl waymark_state_manager_core::Factory for RecordingFactory {
    type Key = u64;
    type Value = ();
    type Error = std::convert::Infallible;

    async fn produce(&self, key: &u64) -> Result<(), Self::Error> {
        self.events.lock().unwrap().push(format!("produce:{key}"));
        Ok(())
    }
}

type TestRequesterProvider = Box<dyn Fn(&u64) -> CapturingRequester + Send + Sync>;
type TestFactory =
    ReconcilingFactory<RecordingFactory, MockBackend, RmpCodec, TestRequesterProvider>;
type FailingRequesterProvider = Box<dyn Fn(&u64) -> FailingRequester + Send + Sync>;
type FailingDeliveryFactory =
    ReconcilingFactory<RecordingFactory, MockBackend, RmpCodec, FailingRequesterProvider>;

struct Harness {
    backend: Arc<MockBackend>,
    factory: TestFactory,
    requester: CapturingRequester,
    events: Arc<Mutex<Vec<String>>>,
    held_locks_rx: tokio::sync::mpsc::UnboundedReceiver<HeldLock<u64>>,
}

fn harness() -> Harness {
    let backend = Arc::new(MockBackend::default());
    let events: Arc<Mutex<Vec<String>>> = Arc::default();
    let requester = CapturingRequester::default();
    let provider_requester = requester.clone();
    let provider_events = Arc::clone(&events);
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    let factory = ReconcilingFactory {
        inner: RecordingFactory {
            events: Arc::clone(&events),
        },
        backend: Arc::clone(&backend),
        codec: RmpCodec,
        lock_owner_id: 7u32,
        lock_time_to_live: LOCK_TIME_TO_LIVE.try_into().unwrap(),
        held_locks_tx,
        requester_provider: Box::new(move |_vm_id: &u64| {
            provider_events.lock().unwrap().push("redeliver".to_owned());
            provider_requester.clone()
        }) as TestRequesterProvider,
    };
    Harness {
        backend,
        factory,
        requester,
        events,
        held_locks_rx,
    }
}

/// Seed a stored request row with an rmp-encoded payload.
fn seed_row(
    backend: &MockBackend,
    vm_id: u64,
    promise: usize,
    effect: usize,
    arguments: Vec<i64>,
    locked_by: Option<u32>,
    expires_in: Duration,
) {
    let payload = ActionCallRequestPayload {
        action_ref: action_ref("greet"),
        arguments,
    };
    let mut blob = Vec::new();
    waymark_vm_codec_core::SerializerProvider::with_serializer(
        &RmpCodec,
        &mut blob,
        |serializer| serde::Serialize::serialize(&payload, serializer),
    )
    .expect("encode payload");

    backend.rows.lock().unwrap().insert(
        TestKey {
            vm_id,
            promise_state_id: PromiseStateId(promise),
        },
        MockRow {
            effect_number: EffectNumber(effect),
            request: blob,
            locked_by,
            lock_expires_at: locked_by.map(|_| Utc::now() + expires_in),
        },
    );
}

#[tokio::test]
async fn reconciles_before_producing() {
    let mut h = harness();
    // A lapsed lock (dead process) — eligible for redelivery.
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64, 6i64],
        Some(99),
        Duration::minutes(-5),
    );

    h.factory.produce(&42u64).await.expect("produce");

    assert_eq!(
        *h.events.lock().unwrap(),
        vec!["redeliver".to_owned(), "produce:42".to_owned()]
    );

    // The stored payload round-tripped into a delivery with the row's
    // correlation.
    let requests = h.requester.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].action_ref, action_ref("greet"));
    assert_eq!(requests[0].arguments, vec![5, 6]);
    assert_eq!(requests[0].metadata.effect_number, EffectNumber(3));
    assert_eq!(requests[0].metadata.promise_state_id, PromiseStateId(11));
    drop(requests);

    // The row is now locked by this process and tracked for renewal.
    let rows = h.backend.rows.lock().unwrap();
    let row = &rows[&TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(11),
    }];
    assert_eq!(row.locked_by, Some(7));
    drop(rows);
    let held_lock = h.held_locks_rx.try_recv().expect("held lock");
    assert_eq!(
        held_lock.key,
        TestKey {
            vm_id: 42,
            promise_state_id: PromiseStateId(11),
        }
    );
}

#[tokio::test]
async fn requests_held_by_a_live_owner_are_left_alone() {
    let mut h = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(5),
    );

    h.factory.produce(&42u64).await.expect("produce");

    assert!(h.requester.requests.lock().unwrap().is_empty());
    assert!(h.held_locks_rx.try_recv().is_err());
    let rows = h.backend.rows.lock().unwrap();
    let row = &rows[&TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(11),
    }];
    assert_eq!(row.locked_by, Some(99));
}

#[tokio::test]
async fn reconcile_failure_fails_the_spawn() {
    let h = harness();
    *h.backend.fail_locks.lock().unwrap() = true;

    let error = h
        .factory
        .produce(&42u64)
        .await
        .expect_err("reconcile failure must fail the spawn");
    assert!(matches!(error, Error::Reconcile(_)));
    assert!(h.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reconciles_every_eligible_row() {
    let mut h = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![1i64],
        Some(99),
        Duration::minutes(-5),
    );
    seed_row(
        &h.backend,
        42,
        12,
        4,
        vec![2i64],
        Some(99),
        Duration::minutes(-5),
    );

    h.factory.produce(&42u64).await.expect("produce");

    let requests = h.requester.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let mut delivered: Vec<_> = requests
        .iter()
        .map(|request| request.metadata.promise_state_id)
        .collect();
    delivered.sort();
    assert_eq!(delivered, vec![PromiseStateId(11), PromiseStateId(12)]);
    drop(requests);

    let mut tracked = vec![
        h.held_locks_rx.try_recv().expect("first held lock").key,
        h.held_locks_rx.try_recv().expect("second held lock").key,
    ];
    tracked.sort_by_key(|key| key.promise_state_id);
    assert_eq!(
        tracked,
        vec![
            TestKey {
                vm_id: 42,
                promise_state_id: PromiseStateId(11),
            },
            TestKey {
                vm_id: 42,
                promise_state_id: PromiseStateId(12),
            },
        ]
    );
}

#[tokio::test]
async fn corrupt_stored_payload_fails_the_spawn() {
    let h = harness();
    h.backend.rows.lock().unwrap().insert(
        TestKey {
            vm_id: 42,
            promise_state_id: PromiseStateId(11),
        },
        MockRow {
            effect_number: EffectNumber(3),
            request: b"not a valid payload".to_vec(),
            locked_by: None,
            lock_expires_at: None,
        },
    );

    let error = h
        .factory
        .produce(&42u64)
        .await
        .expect_err("a corrupt payload must fail the spawn");
    assert!(matches!(
        error,
        Error::Reconcile(ReconcileVmError::Decode(_))
    ));
    assert!(
        h.events
            .lock()
            .unwrap()
            .iter()
            .all(|event| event != "produce:42")
    );
    assert!(h.requester.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn redelivery_failure_fails_the_spawn() {
    let backend = Arc::new(MockBackend::default());
    let events: Arc<Mutex<Vec<String>>> = Arc::default();
    let (held_locks_tx, mut held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    let factory: FailingDeliveryFactory = ReconcilingFactory {
        inner: RecordingFactory {
            events: Arc::clone(&events),
        },
        backend: Arc::clone(&backend),
        codec: RmpCodec,
        lock_owner_id: 7u32,
        lock_time_to_live: LOCK_TIME_TO_LIVE.try_into().unwrap(),
        held_locks_tx,
        requester_provider: Box::new(|_vm_id: &u64| FailingRequester),
    };
    seed_row(
        &backend,
        42,
        11,
        3,
        vec![1i64],
        Some(99),
        Duration::minutes(-5),
    );

    let error = factory
        .produce(&42u64)
        .await
        .expect_err("a failed redelivery must fail the spawn");
    assert!(matches!(
        error,
        Error::Reconcile(ReconcileVmError::Deliver(_))
    ));
    assert!(events.lock().unwrap().is_empty());
    assert!(held_locks_rx.try_recv().is_err());
}
