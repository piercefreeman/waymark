use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::renewal::{Error, HeldLock, Params, run};
use crate::test_support::{MockBackend, MockRow, TestKey};

const HEARTBEAT: Duration = Duration::from_millis(5);

fn key(promise: usize) -> TestKey {
    TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(promise),
    }
}

fn held(key: TestKey) -> HeldLock<u64> {
    HeldLock {
        key,
        taken_at: Instant::now(),
    }
}

fn seed_locked_row(backend: &MockBackend, key: TestKey, locked_by: u32) {
    backend.rows.lock().unwrap().insert(
        key,
        MockRow {
            effect_number: EffectNumber(0),
            request: Vec::new(),
            locked_by: Some(locked_by),
            // About to expire: renewal must push this out.
            lock_expires_at: Some(Utc::now()),
        },
    );
}

fn params(
    backend: &Arc<MockBackend>,
    lock_time_to_live: Duration,
    held_locks_rx: tokio::sync::mpsc::UnboundedReceiver<HeldLock<u64>>,
) -> Params<MockBackend> {
    Params {
        backend: Arc::clone(backend),
        lock_owner_id: 7u32,
        lock_time_to_live: lock_time_to_live.try_into().unwrap(),
        heartbeat: HEARTBEAT.try_into().unwrap(),
        held_locks_rx,
    }
}

#[tokio::test]
async fn renews_prunes_missing_and_drains() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();
    // No row for this key: reported missing, pruned quietly.
    held_locks_tx.send(held(key(3))).unwrap();

    let renewal = tokio::spawn(run(params(
        &backend,
        Duration::from_secs(60),
        held_locks_rx,
    )));

    // Wait until the heartbeat has renewed at least once.
    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // The owned lock was pushed out.
    {
        let rows = backend.rows.lock().unwrap();
        let renewed_expiry = rows[&key(1)].lock_expires_at.expect("locked");
        assert!(renewed_expiry > Utc::now() + chrono::Duration::seconds(30));
    }

    // Simulate the schema trigger: the completion gets recorded, the row
    // vanishes.  With the channel closed, the loop drains and stops.
    drop(held_locks_tx);
    backend.rows.lock().unwrap().remove(&key(1));

    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop drains once all tracked locks are gone")
        .expect("renewal loop task")
        .expect("drain is a peaceful stop");
}

#[tokio::test]
async fn unrenewable_locks_breach_the_fence() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);
    *backend.fail_renewals.lock().unwrap() = true;

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(params(&backend, Duration::from_millis(50), held_locks_rx)),
    )
    .await
    .expect("fence must breach within the time-to-live");

    let Err(Error::FenceBreached(keys)) = outcome else {
        panic!("expected a fence breach, got {outcome:?}");
    };
    assert_eq!(keys.into_iter().collect::<Vec<_>>(), vec![key(1)]);
}

#[tokio::test]
async fn locks_taken_by_another_owner_breach_the_fence() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 99);

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(params(&backend, Duration::from_secs(60), held_locks_rx)),
    )
    .await
    .expect("the first renewal pass must report the loss");

    let Err(Error::HeldElsewhere(keys)) = outcome else {
        panic!("expected a held-elsewhere breach, got {outcome:?}");
    };
    assert_eq!(keys.into_iter().collect::<Vec<_>>(), vec![key(1)]);
}

/// Unconfirmed renewals keep the lock tracked with its existing fence
/// deadline: no breach, no pruning — the next heartbeat retries, and a
/// later confirmed renewal pushes the deadline out again.
#[tokio::test]
async fn unconfirmed_renewals_are_retried_within_the_fence() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);
    *backend.report_unconfirmed_renewals.lock().unwrap() = true;

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let renewal = tokio::spawn(run(params(
        &backend,
        Duration::from_secs(60),
        held_locks_rx,
    )));

    // Several unconfirmed passes: the lock stays tracked, nothing breaches.
    while *backend.renew_calls.lock().unwrap() < 3 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // Recovery: a confirmed renewal pushes the expiry out.
    *backend.report_unconfirmed_renewals.lock().unwrap() = false;
    let recovered_calls = *backend.renew_calls.lock().unwrap() + 1;
    while *backend.renew_calls.lock().unwrap() < recovered_calls {
        tokio::time::sleep(HEARTBEAT).await;
    }
    {
        let rows = backend.rows.lock().unwrap();
        let renewed_expiry = rows[&key(1)].lock_expires_at.expect("locked");
        assert!(renewed_expiry > Utc::now() + chrono::Duration::seconds(30));
    }

    // And the loop still drains peacefully.
    drop(held_locks_tx);
    backend.rows.lock().unwrap().remove(&key(1));
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop drains once all tracked locks are gone")
        .expect("renewal loop task")
        .expect("no breach: unconfirmed renewals stay within the fence");
}

#[tokio::test]
async fn renewal_failures_within_the_fence_are_survived() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);
    *backend.fail_renewals.lock().unwrap() = true;

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let renewal = tokio::spawn(run(params(
        &backend,
        Duration::from_secs(60),
        held_locks_rx,
    )));

    // Let a few renewal attempts fail, well within the time-to-live.
    while *backend.renew_calls.lock().unwrap() < 3 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // Recovery: the next passes renew again and the lock is pushed out.
    *backend.fail_renewals.lock().unwrap() = false;
    let recovered_calls = *backend.renew_calls.lock().unwrap() + 1;
    while *backend.renew_calls.lock().unwrap() < recovered_calls {
        tokio::time::sleep(HEARTBEAT).await;
    }
    {
        let rows = backend.rows.lock().unwrap();
        let renewed_expiry = rows[&key(1)].lock_expires_at.expect("locked");
        assert!(renewed_expiry > Utc::now() + chrono::Duration::seconds(30));
    }

    // And the loop still drains peacefully.
    drop(held_locks_tx);
    backend.rows.lock().unwrap().remove(&key(1));
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop drains once all tracked locks are gone")
        .expect("renewal loop task")
        .expect("no breach: the failures stayed within the fence");
}
