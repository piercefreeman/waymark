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

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        std::future::pending(),
    ));

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
        run(
            params(&backend, Duration::from_millis(50), held_locks_rx),
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
    seed_locked_row(&backend, key(1), 99);

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        run(
            params(&backend, Duration::from_secs(60), held_locks_rx),
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
    seed_locked_row(&backend, key(1), 7);
    *backend.report_unconfirmed_renewals.lock().unwrap() = true;

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        std::future::pending(),
    ));

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

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        std::future::pending(),
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

    // And the loop still drains peacefully.
    drop(held_locks_tx);
    backend.rows.lock().unwrap().remove(&key(1));
    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop drains once all tracked locks are gone")
        .expect("renewal loop task")
        .expect("no breach: the failures stayed within the fence");
}

/// Paused-time parameters: a heartbeat a minute apart, so a stop that
/// waits for the next heartbeat is told apart from one that does not.
fn slow_params(
    backend: &Arc<MockBackend>,
    held_locks_rx: tokio::sync::mpsc::UnboundedReceiver<HeldLock<u64>>,
) -> Params<MockBackend> {
    Params {
        backend: Arc::clone(backend),
        lock_owner_id: 7u32,
        lock_time_to_live: Duration::from_secs(600).try_into().unwrap(),
        heartbeat: Duration::from_secs(60).try_into().unwrap(),
        held_locks_rx,
    }
}

/// The channel closing with nothing tracked stops the loop at once, not
/// at the next heartbeat.
#[tokio::test(start_paused = true)]
async fn a_closed_channel_with_nothing_tracked_stops_at_once() {
    let backend = Arc::new(MockBackend::default());
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();

    let renewal = tokio::spawn(run(
        slow_params(&backend, held_locks_rx),
        std::future::pending(),
    ));

    // Past the interval's first tick; the next one is a minute out.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!renewal.is_finished());

    drop(held_locks_tx);

    tokio::time::timeout(Duration::from_secs(1), renewal)
        .await
        .expect("the loop stops without waiting for the next heartbeat")
        .expect("renewal loop task")
        .expect("drain is a peaceful stop");
}

/// The channel closing with a lock still tracked, then the heartbeat
/// that finds its row gone: the loop stops on that heartbeat, not the
/// next one.
#[tokio::test(start_paused = true)]
async fn the_heartbeat_that_empties_the_tracked_set_stops_at_once() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let renewal = tokio::spawn(run(
        slow_params(&backend, held_locks_rx),
        std::future::pending(),
    ));

    // The lock is tracked and renewed once; the next heartbeat is a
    // minute out.
    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // The channel closes, then the row goes: the next heartbeat empties
    // the tracked set.
    drop(held_locks_tx);
    backend.rows.lock().unwrap().remove(&key(1));

    // Between one and two heartbeats: a stop deferred to the heartbeat
    // after the emptying one misses the bound.
    tokio::time::timeout(Duration::from_secs(90), renewal)
        .await
        .expect("the loop stops on the heartbeat that empties the tracked set")
        .expect("renewal loop task")
        .expect("drain is a peaceful stop");
    assert_eq!(*backend.renew_calls.lock().unwrap(), 2);
}

/// A renewal slower than the heartbeat leaves no backlog of ticks behind
/// it: one renewal follows the stalled one at once, and the next waits for
/// the grid point. `Burst` would fire the missed ticks back to back;
/// `Delay` would put the next renewal a full heartbeat after the stall.
#[tokio::test(start_paused = true)]
async fn a_slow_renewal_skips_the_heartbeats_it_held_up() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);
    // The first renewal stalls for two and a half heartbeats.
    *backend.renew_delay.lock().unwrap() = Some(Duration::from_secs(150));
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();

    let renewal = tokio::spawn(run(
        slow_params(&backend, held_locks_rx),
        std::future::pending(),
    ));

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

    drop(held_locks_tx);
    backend.rows.lock().unwrap().remove(&key(1));
    tokio::time::timeout(Duration::from_secs(90), renewal)
        .await
        .expect("the loop stops on the heartbeat that finds the row gone")
        .expect("renewal loop task")
        .expect("drain is a peaceful stop");
}

/// The shutdown future resolving after the rows are gone, but before a
/// heartbeat noticed, is still a peaceful stop: the last heartbeat lets
/// them leave.
#[tokio::test]
async fn shutdown_after_the_rows_are_gone_is_peaceful() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    while *backend.renew_calls.lock().unwrap() == 0 {
        tokio::time::sleep(HEARTBEAT).await;
    }

    // The completion gets recorded and the shutdown follows at once,
    // with the channel still open.
    backend.rows.lock().unwrap().remove(&key(1));
    shutdown_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("every lock was accounted for by the last heartbeat");
    drop(held_locks_tx);
}

/// The shutdown future resolving with nothing tracked is a peaceful stop
/// with no heartbeat: there is nothing to renew or to abandon.
#[tokio::test]
async fn shutdown_with_nothing_tracked_stops_at_once() {
    let backend = Arc::new(MockBackend::default());
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    shutdown_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(5), renewal)
        .await
        .expect("renewal loop stops on the shutdown future")
        .expect("renewal loop task")
        .expect("nothing tracked is a peaceful stop");
    assert_eq!(*backend.renew_calls.lock().unwrap(), 0);
    drop(held_locks_tx);
}

/// The shutdown future resolving with locks still held stops the loop
/// with those locks named: they are left to lapse by their time-to-live.
#[tokio::test]
async fn shutdown_with_locks_still_held_abandons_them() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        async move {
            let _ = shutdown_rx.await;
        },
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
    drop(held_locks_tx);
}

/// The last heartbeat's renewal call failing names every tracked lock as
/// abandoned, a row already gone included: nothing was learnt about it.
#[tokio::test]
async fn shutdown_with_the_last_renewal_failing_abandons_every_tracked_lock() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);

    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let renewal = tokio::spawn(run(
        params(&backend, Duration::from_secs(60), held_locks_rx),
        async move {
            let _ = shutdown_rx.await;
        },
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
    drop(held_locks_tx);
}

/// A lock the last heartbeat on shutdown finds taken by another owner is
/// reported as that breach, not folded into `Abandoned`.
#[tokio::test(start_paused = true)]
async fn shutdown_reports_a_lock_the_last_heartbeat_finds_taken_elsewhere() {
    let backend = Arc::new(MockBackend::default());
    seed_locked_row(&backend, key(1), 7);
    let (held_locks_tx, held_locks_rx) = tokio::sync::mpsc::unbounded_channel();
    held_locks_tx.send(held(key(1))).unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let renewal = tokio::spawn(run(slow_params(&backend, held_locks_rx), async move {
        let _ = shutdown_rx.await;
    }));

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
    drop(held_locks_tx);
}
