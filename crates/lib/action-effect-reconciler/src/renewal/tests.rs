use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::action_call_request_locks::Tracker;
use crate::renewal::{Error, Params, run};
use crate::test_support::{LOCK_OWNER, MockBackend, MockRow, TestKey, shared_tracker};

const HEARTBEAT: Duration = Duration::from_millis(5);

fn key(promise: usize) -> TestKey {
    TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(promise),
    }
}

/// Track the lock of `key`, taken now with the given time-to-live, its
/// call requested.
fn track(tracker: &Arc<std::sync::Mutex<Tracker<u64>>>, key: TestKey, time_to_live: Duration) {
    Tracker::track_lock(tracker, key, Instant::now() + time_to_live).confirm_requested();
}

/// Track the lock of `key`, taken now, its call never going through.
fn track_never_requested(tracker: &Arc<std::sync::Mutex<Tracker<u64>>>, key: TestKey) {
    drop(Tracker::track_lock(
        tracker,
        key,
        Instant::now() + Duration::from_secs(60),
    ));
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
    tracker: &Arc<std::sync::Mutex<Tracker<u64>>>,
) -> Params<MockBackend> {
    Params {
        backend: Arc::clone(backend),
        lock_owner_id: LOCK_OWNER,
        lock_time_to_live: lock_time_to_live.try_into().unwrap(),
        heartbeat: HEARTBEAT.try_into().unwrap(),
        action_call_request_locks_tracker: Arc::clone(tracker),
    }
}

/// Paused-time parameters: a heartbeat a minute apart, so a stop that
/// waits for the next heartbeat is told apart from one that does not.
fn slow_params(
    backend: &Arc<MockBackend>,
    tracker: &Arc<std::sync::Mutex<Tracker<u64>>>,
) -> Params<MockBackend> {
    Params {
        backend: Arc::clone(backend),
        lock_owner_id: LOCK_OWNER,
        lock_time_to_live: Duration::from_secs(600).try_into().unwrap(),
        heartbeat: Duration::from_secs(60).try_into().unwrap(),
        action_call_request_locks_tracker: Arc::clone(tracker),
    }
}

/// A shutdown future and the sender resolving it.
fn shutdown() -> (tokio::sync::oneshot::Sender<()>, impl Future<Output = ()>) {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    (shutdown_tx, async move {
        let _ = shutdown_rx.await;
    })
}

#[tokio::test]
async fn renews_and_prunes_missing() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    track(&tracker, key(1), Duration::from_secs(60));
    // No row for this key: reported missing, pruned quietly.
    track(&tracker, key(3), Duration::from_secs(60));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    // Wait until the heartbeat has renewed at least once.
    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // The owned lock was pushed out; the missing one was pruned.
    {
        let rows = backend.rows.lock().unwrap();
        let renewed_expiry = rows[&key(1)].lock_expires_at.expect("locked");
        assert!(renewed_expiry > Utc::now() + chrono::Duration::seconds(30));
    }
    assert_eq!(tracker.lock().unwrap().tracked_keys(), vec![key(1)]);

    // Simulate the schema trigger: the completion gets recorded, the row
    // vanishes.  The shutdown's last heartbeat then finds every lock gone.
    backend.rows.lock().unwrap().remove(&key(1));
    shutdown_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("every lock was accounted for by the last heartbeat");
}

/// The locks of requests that never went through are unlocked on the next
/// heartbeat and never tracked.
#[tokio::test]
async fn unlocks_the_requests_that_never_went_through() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    track_never_requested(&tracker, key(1));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    while *backend.unlock_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }
    tokio::time::sleep(HEARTBEAT).await;

    {
        let rows = backend.rows.lock().unwrap();
        assert_eq!(rows[&key(1)].locked_by, None);
    }
    {
        let tracker = tracker.lock().unwrap();
        assert!(tracker.pending_unlocks().is_none());
        assert!(!tracker.is_tracking(&key(1)));
    }
    assert_eq!(*backend.renew_calls.lock().unwrap(), 0);

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("nothing tracked is a peaceful stop");
}

/// A failed unlock call leaves the keys pending; the next heartbeat
/// retries.
#[tokio::test]
async fn unlock_failures_are_retried_at_the_next_heartbeat() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    *backend.fail_unlocks.lock().unwrap() = true;
    track_never_requested(&tracker, key(1));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    while *backend.unlock_calls.lock().unwrap() < 3 {
        tokio::time::sleep(HEARTBEAT).await;
    }
    assert!(tracker.lock().unwrap().pending_unlocks().is_some());
    assert_eq!(
        backend.rows.lock().unwrap()[&key(1)].locked_by,
        Some(LOCK_OWNER)
    );

    // Recovery: the next heartbeat's unlock goes through.
    *backend.fail_unlocks.lock().unwrap() = false;
    let recovered_calls = *backend.unlock_calls.lock().unwrap() + 1;
    while *backend.unlock_calls.lock().unwrap() < recovered_calls {
        tokio::time::sleep(HEARTBEAT).await;
    }
    tokio::time::sleep(HEARTBEAT).await;
    assert!(tracker.lock().unwrap().pending_unlocks().is_none());
    assert_eq!(backend.rows.lock().unwrap()[&key(1)].locked_by, None);

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("nothing tracked is a peaceful stop");
}

#[tokio::test]
async fn unrenewable_locks_breach_the_fence() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    *backend.fail_renewals.lock().unwrap() = true;
    track(&tracker, key(1), Duration::from_millis(50));

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(
            params(&backend, Duration::from_millis(50), &tracker),
            std::future::pending(),
        ),
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
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), 99);
    track(&tracker, key(1), Duration::from_secs(60));

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(
            params(&backend, Duration::from_secs(60), &tracker),
            std::future::pending(),
        ),
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
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    *backend.report_unconfirmed_renewals.lock().unwrap() = true;
    track(&tracker, key(1), Duration::from_secs(60));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    // Several unconfirmed passes: the lock stays tracked, nothing breaches.
    while *backend.renew_calls.lock().unwrap() < 3 {
        tokio::time::sleep(HEARTBEAT).await;
    }
    assert!(tracker.lock().unwrap().is_tracking(&key(1)));

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

    // And the loop still stops peacefully once the row is gone.
    backend.rows.lock().unwrap().remove(&key(1));
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("no breach: unconfirmed renewals stay within the fence");
}

#[tokio::test]
async fn renewal_failures_within_the_fence_are_survived() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    *backend.fail_renewals.lock().unwrap() = true;
    track(&tracker, key(1), Duration::from_secs(60));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

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

    // And the loop still stops peacefully once the row is gone.
    backend.rows.lock().unwrap().remove(&key(1));
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("no breach: the failures stayed within the fence");
}

/// A renewal slower than the heartbeat leaves no backlog of ticks behind
/// it: one renewal follows the stalled one at once, and the next waits for
/// the grid point. `Burst` would fire the missed ticks back to back;
/// `Delay` would put the next renewal a full heartbeat after the stall.
#[tokio::test(start_paused = true)]
async fn a_slow_renewal_skips_the_heartbeats_it_held_up() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    // The first renewal stalls for two and a half heartbeats.
    *backend.renew_delay.lock().unwrap() = Some(Duration::from_secs(150));
    track(&tracker, key(1), Duration::from_secs(600));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(slow_params(&backend, &tracker), shutdown));

    // Just past the stall: the stalled renewal, then the one immediate
    // post-stall renewal; the two grid points the stall swallowed fire
    // once between them.
    tokio::time::sleep(Duration::from_secs(151)).await;
    assert_eq!(*backend.renew_calls.lock().unwrap(), 2);

    // Nothing until the next grid point, at 180 s.
    tokio::time::sleep(Duration::from_secs(28)).await;
    assert_eq!(*backend.renew_calls.lock().unwrap(), 2);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(*backend.renew_calls.lock().unwrap(), 3);

    backend.rows.lock().unwrap().remove(&key(1));
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(90), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("the last heartbeat found the row gone");
}

/// The shutdown future resolving after the rows are gone, but before a
/// heartbeat noticed, is still a peaceful stop: the last heartbeat lets
/// them leave.
#[tokio::test]
async fn shutdown_after_the_rows_are_gone_is_peaceful() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    track(&tracker, key(1), Duration::from_secs(60));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // The completion gets recorded and the shutdown follows at once.
    backend.rows.lock().unwrap().remove(&key(1));
    shutdown_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("every lock was accounted for by the last heartbeat");
}

/// The shutdown future resolving with nothing tracked is a peaceful stop
/// with no heartbeat: there is nothing to renew or to abandon.
#[tokio::test]
async fn shutdown_with_nothing_tracked_stops_at_once() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    let (shutdown_tx, shutdown) = shutdown();

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    shutdown_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("nothing tracked is a peaceful stop");
    assert_eq!(*backend.renew_calls.lock().unwrap(), 0);
}

/// The shutdown future resolving with locks still held stops the loop
/// with those locks named: they are left to lapse by their time-to-live.
#[tokio::test]
async fn shutdown_with_locks_still_held_abandons_them() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    track(&tracker, key(1), Duration::from_secs(60));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    shutdown_tx.send(()).unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task");
    let Err(Error::Abandoned(keys)) = outcome else {
        panic!("expected the held lock abandoned, got {outcome:?}");
    };
    assert_eq!(keys.into_iter().collect::<Vec<_>>(), vec![key(1)]);
}

/// The last heartbeat's renewal call failing names every tracked lock as
/// abandoned, a row already gone included: nothing was learnt about it.
#[tokio::test]
async fn shutdown_with_the_last_renewal_failing_abandons_every_tracked_lock() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    track(&tracker, key(1), Duration::from_secs(60));

    let (shutdown_tx, shutdown) = shutdown();
    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), &tracker),
        shutdown,
    ));

    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // The row goes away, but the renewal call that would report it gone
    // fails from here on.
    backend.rows.lock().unwrap().remove(&key(1));
    *backend.fail_renewals.lock().unwrap() = true;
    let calls_before_shutdown = *backend.renew_calls.lock().unwrap();
    shutdown_tx.send(()).unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task");
    let Err(Error::Abandoned(keys)) = outcome else {
        panic!("expected the lock abandoned, got {outcome:?}");
    };
    assert_eq!(keys.into_iter().collect::<Vec<_>>(), vec![key(1)]);
    assert!(
        *backend.renew_calls.lock().unwrap() > calls_before_shutdown,
        "the last heartbeat was attempted"
    );
}

/// A lock the last heartbeat on shutdown finds taken by another owner is
/// reported as that breach, not folded into `Abandoned`.
#[tokio::test(start_paused = true)]
async fn shutdown_reports_a_lock_the_last_heartbeat_finds_taken_elsewhere() {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    seed_locked_row(&backend, key(1), LOCK_OWNER);
    track(&tracker, key(1), Duration::from_secs(600));
    let (shutdown_tx, shutdown) = shutdown();

    let renewal = tokio::spawn(run(slow_params(&backend, &tracker), shutdown));

    // The lock is tracked and renewed once; the next heartbeat is a
    // minute out, so the only heartbeat from here is the shutdown's.
    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Another owner takes the row, then the shutdown arrives.
    backend
        .rows
        .lock()
        .unwrap()
        .get_mut(&key(1))
        .unwrap()
        .locked_by = Some(99);
    shutdown_tx.send(()).unwrap();

    let outcome = tokio::time::timeout(Duration::from_secs(1), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task");
    let Err(Error::HeldElsewhere(keys)) = outcome else {
        panic!("expected a held-elsewhere breach, got {outcome:?}");
    };
    assert_eq!(keys.into_iter().collect::<Vec<_>>(), vec![key(1)]);
    assert_eq!(
        *backend.renew_calls.lock().unwrap(),
        2,
        "the last heartbeat made the one renewal call that found the lock taken"
    );
}
