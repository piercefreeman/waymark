use std::sync::Arc;

use chrono::{Duration, Utc};
use nonempty_collections::{NESlice, NEVec, nev};
use waymark_extcall_reconciler_core::ActionPromiseSettler as _;
use waymark_vm_codec_rmp::RmpCodec;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::AdoptingActionSettler;
use crate::action_call_request_locks::Tracker;
use crate::adopting_action_settler::{Adopter, Error, recheck};
use crate::test_support::{
    CapturingRequester, FailingRequester, LOCK_OWNER, LOCK_TIME_TO_LIVE, MockBackend, MockRow,
    TestKey, action_ref, lock_id, seed_row, shared_tracker, spawn_locker, tracks_request,
};

const HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(5);

/// A settlement acknowledgement that records nothing.
struct NoopAck;

impl waymark_vm_driver_core::PromiseSettlementAck for NoopAck {
    fn acknowledge_promise_settlement(self) {}
}

type TestSettlement = PromiseSettlement<i64, String, NoopAck>;

#[derive(Debug, thiserror::Error)]
#[error("mock settler failure")]
struct MockSettleError;

/// The decorated settler: hands out the settlements fed through its
/// channel, one batch per poll, and waits while the channel is empty.
///
/// A hand-rolled double: the settler trait's method returns a borrowing
/// `impl Future` under a generic unified-ack parameter, which `mockall`
/// does not model.
struct ChannelSettler {
    settlements_rx: tokio::sync::mpsc::UnboundedReceiver<NEVec<TestSettlement>>,
    polls: Arc<std::sync::Mutex<Vec<Vec<PromiseStateId>>>>,
}

impl waymark_extcall_reconciler_core::SettlerAck for ChannelSettler {
    type Ack = NoopAck;
}

impl waymark_extcall_reconciler_core::HasValue for ChannelSettler {
    type Value = i64;
}

impl waymark_extcall_reconciler_core::HasRaisedException for ChannelSettler {
    type RaisedException = String;
}

impl waymark_extcall_reconciler_core::ActionPromiseSettler<NoopAck> for ChannelSettler {
    type Error = MockSettleError;

    async fn poll_action_settlements<'a>(
        &'a mut self,
        waiting_promise_state_ids: NESlice<'a, PromiseStateId>,
    ) -> Result<NEVec<TestSettlement>, MockSettleError>
    where
        NoopAck: 'a,
    {
        self.polls
            .lock()
            .unwrap()
            .push(waiting_promise_state_ids.iter().copied().collect());

        self.settlements_rx.recv().await.ok_or(MockSettleError)
    }
}

fn settlement(promise: usize) -> NEVec<TestSettlement> {
    nev![PromiseSettlement {
        promise_state_id: PromiseStateId(promise),
        resolution: PromiseResolution::Resolved(1),
        ack: NoopAck,
    }]
}

struct Harness<ActionCallRequester> {
    backend: Arc<MockBackend>,
    tracker: Arc<std::sync::Mutex<Tracker<u64>>>,
    settler: AdoptingActionSettler<ChannelSettler, u64, RmpCodec, ActionCallRequester>,
    settlements_tx: tokio::sync::mpsc::UnboundedSender<NEVec<TestSettlement>>,
    polls: Arc<std::sync::Mutex<Vec<Vec<PromiseStateId>>>>,
    recheck: recheck::Tx,
}

fn harness_with<ActionCallRequester>(
    requester: ActionCallRequester,
) -> Harness<ActionCallRequester> {
    let backend = Arc::new(MockBackend::default());
    let tracker = shared_tracker();
    let (settlements_tx, settlements_rx) = tokio::sync::mpsc::unbounded_channel();
    let polls: Arc<std::sync::Mutex<Vec<Vec<PromiseStateId>>>> = Arc::default();
    let (recheck, recheck_rx) = recheck::new();
    let settler = AdoptingActionSettler {
        inner: ChannelSettler {
            settlements_rx,
            polls: Arc::clone(&polls),
        },
        adopter: Adopter::new(
            42u64,
            spawn_locker(&backend, &tracker),
            RmpCodec,
            requester,
            Arc::clone(&tracker),
            recheck_rx,
            HEARTBEAT.try_into().unwrap(),
        ),
    };
    Harness {
        backend,
        tracker,
        settler,
        settlements_tx,
        polls,
        recheck,
    }
}

fn harness() -> (Harness<CapturingRequester>, CapturingRequester) {
    let requester = CapturingRequester::default();
    (harness_with(requester.clone()), requester)
}

fn key(promise: usize) -> TestKey {
    TestKey {
        vm_id: 42,
        promise_state_id: PromiseStateId(promise),
    }
}

/// Poll the settler for the given promises, with a settlement already
/// available so the wait returns.
async fn poll_settled<ActionCallRequester>(
    h: &mut Harness<ActionCallRequester>,
    waiting: &[PromiseStateId],
    settled: usize,
) -> NEVec<TestSettlement>
where
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<
            Metadata = waymark_action_runtime_metadata::ActionCallCorrelation,
            Argument = i64,
        >,
    ActionCallRequester: Send + Sync,
{
    h.settlements_tx.send(settlement(settled)).unwrap();
    h.settler
        .poll_action_settlements(NESlice::try_from_slice(waiting).unwrap())
        .await
        .expect("the wait settles")
}

/// Expire the lock on the row of `promise`.  Tokio's paused clock does
/// not move the wall clock the mock compares against, so the row's
/// expiry is moved instead.
fn expire_lock(backend: &MockBackend, promise: usize) {
    backend
        .rows
        .lock()
        .unwrap()
        .get_mut(&key(promise))
        .unwrap()
        .lock_expires_at = Some(Utc::now() - Duration::seconds(1));
}

#[tokio::test(start_paused = true)]
async fn first_wait_adopts_lapsed_rows() {
    let (mut h, requester) = harness();
    // Two lapsed locks (dead process) — eligible for adoption.
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64, 6i64],
        Some(99),
        Duration::minutes(-5),
    );
    seed_row(
        &h.backend,
        42,
        12,
        4,
        vec![7i64],
        Some(99),
        Duration::minutes(-5),
    );

    let settlements = poll_settled(&mut h, &[PromiseStateId(11), PromiseStateId(12)], 11).await;
    assert_eq!(settlements.len().get(), 1);

    // The stored payloads round-tripped into deliveries with the rows'
    // correlation, before the decorated settler was polled.
    {
        let mut requests = requester.requests.lock().unwrap();
        requests.sort_by_key(|request| request.metadata.promise_state_id);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].action_ref, action_ref("greet"));
        assert_eq!(requests[0].arguments, vec![5, 6]);
        assert_eq!(requests[0].metadata.effect_number, EffectNumber(3));
        assert_eq!(requests[0].metadata.promise_state_id, PromiseStateId(11));
        assert_eq!(requests[1].arguments, vec![7]);
        assert_eq!(requests[1].metadata.promise_state_id, PromiseStateId(12));
    }
    assert_eq!(
        *h.polls.lock().unwrap(),
        vec![vec![PromiseStateId(11), PromiseStateId(12)]]
    );

    // The rows are now locked by this process and tracked for renewal.
    {
        let rows = h.backend.rows.lock().unwrap();
        assert_eq!(rows[&key(11)].locked_by, Some(LOCK_OWNER));
        assert_eq!(rows[&key(12)].locked_by, Some(LOCK_OWNER));
    }
    let tracker = h.tracker.lock().unwrap();
    assert!(tracks_request(&tracker, &key(11)));
    assert!(tracks_request(&tracker, &key(12)));
    assert!(tracker.pending_unlocks().is_none());
}

#[tokio::test(start_paused = true)]
async fn rows_held_by_a_live_owner_are_left_alone() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(5),
    );

    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;

    assert!(requester.requests.lock().unwrap().is_empty());
    assert!(!tracks_request(&h.tracker.lock().unwrap(), &key(11)));
    assert_eq!(h.backend.rows.lock().unwrap()[&key(11)].locked_by, Some(99));
}

/// The passes of one wait share one poll of the decorated settler.
#[tokio::test(start_paused = true)]
async fn the_passes_of_a_wait_share_one_poll() {
    let (mut h, _requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(5),
    );
    let waiting = [PromiseStateId(11)];

    let wait = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&waiting).unwrap());
    let mut wait = std::pin::pin!(wait);
    // A pass is submitted to the lock batcher on one poll and lands on
    // the next.
    assert!(poll_once_is_pending(&mut wait).await);
    assert!(poll_once_is_pending(&mut wait).await);
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    // Three heartbeats, three more passes; one poll throughout.
    for passes in 2..=4 {
        tokio::time::sleep(HEARTBEAT).await;
        assert!(poll_once_is_pending(&mut wait).await);
        assert!(poll_once_is_pending(&mut wait).await);
        assert_eq!(*h.backend.lock_calls.lock().unwrap(), passes);
    }
    assert_eq!(*h.polls.lock().unwrap(), vec![vec![PromiseStateId(11)]]);

    h.settlements_tx.send(settlement(11)).unwrap();
    let settlements = wait.await.expect("the wait settles");
    assert_eq!(settlements.len().get(), 1);
}

/// Issue #832: the VM is revived here while its previous owner still
/// holds an action's lock; the owner dies without completing it.
#[tokio::test(start_paused = true)]
async fn adopts_a_row_whose_lock_lapses_during_the_wait() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(5),
    );
    let waiting = [PromiseStateId(11)];

    // The first pass finds the row held by the live owner 99.
    let wait = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&waiting).unwrap());
    let mut wait = std::pin::pin!(wait);
    assert!(poll_once_is_pending(&mut wait).await);
    assert!(requester.requests.lock().unwrap().is_empty());

    // Owner 99 dies without completing or unlocking: its lock lapses.
    expire_lock(&h.backend, 11);

    // One heartbeat later the wait's pass adopts the row.
    tokio::time::sleep(HEARTBEAT).await;
    assert!(poll_once_is_pending(&mut wait).await);
    {
        let requests = requester.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].arguments, vec![5]);
        assert_eq!(requests[0].metadata.promise_state_id, PromiseStateId(11));
    }
    assert_eq!(
        h.backend.rows.lock().unwrap()[&key(11)].locked_by,
        Some(LOCK_OWNER)
    );
    assert!(tracks_request(&h.tracker.lock().unwrap(), &key(11)));

    // The adopted attempt's completion settles the wait.
    h.settlements_tx.send(settlement(11)).unwrap();
    let settlements = wait.await.expect("the wait settles");
    assert_eq!(settlements.len().get(), 1);
}

/// The gap of a recording whose statement committed but reported failure:
/// the row sits locked here under a lock nobody tracks, the VM is already
/// covered, and the effect handler's recheck is the only way back to a
/// pass.
#[tokio::test(start_paused = true)]
async fn a_recheck_reopens_a_covered_vm_and_adopts_the_untracked_row() {
    let (mut h, requester) = harness();

    // No rows: the first wait covers the VM.
    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    // A row locked by this process under a lock the tracker never saw;
    // the effect handler found it already recorded and requested a recheck.
    seed_row(
        &h.backend,
        42,
        12,
        4,
        vec![5i64],
        Some(LOCK_OWNER),
        Duration::minutes(5),
    );
    h.recheck.request_recheck();

    // The next wait runs a pass again: the row is held here untracked, so
    // the VM watches it.
    let waiting = [PromiseStateId(12)];
    let wait = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&waiting).unwrap());
    let mut wait = std::pin::pin!(wait);
    assert!(poll_once_is_pending(&mut wait).await);
    assert!(poll_once_is_pending(&mut wait).await);
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);
    assert!(requester.requests.lock().unwrap().is_empty());

    // Once the untracked lock lapses, the pass adopts the row.
    expire_lock(&h.backend, 12);
    tokio::time::sleep(HEARTBEAT).await;
    assert!(poll_once_is_pending(&mut wait).await);
    assert!(poll_once_is_pending(&mut wait).await);
    {
        let requests = requester.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].metadata.promise_state_id, PromiseStateId(12));
    }
    assert!(tracks_request(&h.tracker.lock().unwrap(), &key(12)));

    h.settlements_tx.send(settlement(12)).unwrap();
    let settlements = wait.await.expect("the wait settles");
    assert_eq!(settlements.len().get(), 1);
}

/// A recheck requested of a covered VM while its wait is already pending
/// runs a pass inside that wait.
#[tokio::test(start_paused = true)]
async fn a_recheck_during_a_covered_wait_runs_a_pass_in_it() {
    let (mut h, _requester) = harness();

    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    let waiting = [PromiseStateId(12)];
    let wait = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&waiting).unwrap());
    let mut wait = std::pin::pin!(wait);
    assert!(poll_once_is_pending(&mut wait).await);
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    h.recheck.request_recheck();
    assert!(poll_once_is_pending(&mut wait).await);
    assert!(poll_once_is_pending(&mut wait).await);
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);

    h.settlements_tx.send(settlement(12)).unwrap();
    let settlements = wait.await.expect("the wait settles");
    assert_eq!(settlements.len().get(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_recheck_on_a_covered_vm_costs_one_pass() {
    let (mut h, _requester) = harness();

    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    // A replayed effect requested a recheck; the pass finds nothing to
    // adopt and the VM is covered again.
    h.recheck.request_recheck();
    poll_settled(&mut h, &[PromiseStateId(12)], 12).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);
    poll_settled(&mut h, &[PromiseStateId(13)], 13).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn stops_watching_once_the_row_is_gone() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(5),
    );

    // Owner 99 completes the attempt: the store removes the row and the
    // completion settles the wait.
    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    h.backend.rows.lock().unwrap().remove(&key(11));
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    // The pass due one heartbeat later finds nothing held elsewhere; no
    // wait after it issues a lock call.
    tokio::time::sleep(HEARTBEAT).await;
    poll_settled(&mut h, &[PromiseStateId(12)], 12).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);
    tokio::time::sleep(3 * HEARTBEAT).await;
    poll_settled(&mut h, &[PromiseStateId(13)], 13).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);
    assert!(requester.requests.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn covered_waits_issue_no_lock_call() {
    let (mut h, _requester) = harness();

    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    poll_settled(&mut h, &[PromiseStateId(12)], 12).await;
    tokio::time::sleep(3 * HEARTBEAT).await;
    poll_settled(&mut h, &[PromiseStateId(13)], 13).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);
}

/// A row this process holds and tracks is an attempt running here: the
/// VM is covered.
#[tokio::test(start_paused = true)]
async fn a_tracked_row_held_here_is_covered() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(LOCK_OWNER),
        Duration::minutes(5),
    );
    Tracker::track_lock(
        &h.tracker,
        lock_id(key(11), 1),
        tokio::time::Instant::now().into_std() + LOCK_TIME_TO_LIVE,
    )
    .confirm_requested();

    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    tokio::time::sleep(3 * HEARTBEAT).await;
    poll_settled(&mut h, &[PromiseStateId(12)], 12).await;

    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);
    assert!(requester.requests.lock().unwrap().is_empty());
}

/// A row this process holds without tracking it is a call that never went
/// through: watched, and adopted once its lock is gone.
#[tokio::test(start_paused = true)]
async fn an_untracked_row_held_here_is_adopted_once_unlocked() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(LOCK_OWNER),
        Duration::minutes(5),
    );
    let waiting = [PromiseStateId(11)];

    let wait = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&waiting).unwrap());
    let mut wait = std::pin::pin!(wait);
    assert!(poll_once_is_pending(&mut wait).await);
    assert!(requester.requests.lock().unwrap().is_empty());

    // The renewal loop unlocks the row (here: by hand).
    {
        let mut rows = h.backend.rows.lock().unwrap();
        let row = rows.get_mut(&key(11)).unwrap();
        row.locked_by = None;
        row.lock_expires_at = None;
    }

    tokio::time::sleep(HEARTBEAT).await;
    assert!(poll_once_is_pending(&mut wait).await);
    assert_eq!(requester.requests.lock().unwrap().len(), 1);
    assert!(tracks_request(&h.tracker.lock().unwrap(), &key(11)));
}

/// A wait dropped between the lock and the request: the pending call request
/// reports the request as never gone through, the pass's state is left
/// as it was, and the next pass adopts the row once it is unlocked.
#[tokio::test(start_paused = true)]
async fn a_wait_dropped_mid_pass_leaves_the_lock_up_for_unlocking() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(-5),
    );
    let waiting = [PromiseStateId(11)];

    // Poll once: the pass submits to the lock batcher and is parked on
    // its answer, which the batcher's task delivers while the wait is
    // not polled again.
    {
        let wait = h
            .settler
            .poll_action_settlements(NESlice::try_from_slice(&waiting).unwrap());
        let mut wait = std::pin::pin!(wait);
        let mut polled = std::future::poll_fn(|context| {
            std::task::Poll::Ready(wait.as_mut().poll(context).is_pending())
        });
        assert!(std::pin::Pin::new(&mut polled).await);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    // The wait is gone; the row is locked here, the call never requested.
    assert!(requester.requests.lock().unwrap().is_empty());
    assert_eq!(
        h.backend.rows.lock().unwrap()[&key(11)].locked_by,
        Some(LOCK_OWNER)
    );
    {
        let tracker = h.tracker.lock().unwrap();
        assert!(!tracks_request(&tracker, &key(11)));
        let pending = tracker
            .pending_unlocks()
            .expect("the lock is up for unlocking");
        assert_eq!(
            pending
                .lock_ids()
                .iter()
                .map(|lock_id| lock_id.request)
                .collect::<Vec<_>>(),
            vec![key(11)]
        );
    }

    // The renewal loop unlocks it (here: by hand); the next wait's pass
    // adopts it.
    {
        let mut rows = h.backend.rows.lock().unwrap();
        let row = rows.get_mut(&key(11)).unwrap();
        row.locked_by = None;
        row.lock_expires_at = None;
    }
    poll_settled(&mut h, &[PromiseStateId(11)], 11).await;
    assert_eq!(requester.requests.lock().unwrap().len(), 1);
    assert!(tracks_request(&h.tracker.lock().unwrap(), &key(11)));
}

#[tokio::test(start_paused = true)]
async fn frequent_short_waits_do_not_postpone_the_pass() {
    let (mut h, requester) = harness();
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![5i64],
        Some(99),
        Duration::minutes(5),
    );
    poll_settled(&mut h, &[PromiseStateId(11), PromiseStateId(20)], 20).await;
    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 1);

    expire_lock(&h.backend, 11);

    // Waits that settle at once, spaced under a heartbeat, until the
    // deadline stored at the first pass is reached.
    for settled in 21..=25 {
        tokio::time::sleep(HEARTBEAT / 4).await;
        poll_settled(
            &mut h,
            &[PromiseStateId(11), PromiseStateId(settled)],
            settled,
        )
        .await;
    }

    assert_eq!(*h.backend.lock_calls.lock().unwrap(), 2);
    assert_eq!(requester.requests.lock().unwrap().len(), 1);
    assert_eq!(
        h.backend.rows.lock().unwrap()[&key(11)].locked_by,
        Some(LOCK_OWNER)
    );
}

#[tokio::test(start_paused = true)]
async fn lock_failure_fails_the_wait() {
    let (mut h, _requester) = harness();
    *h.backend.fail_locks.lock().unwrap() = true;
    h.settlements_tx.send(settlement(11)).unwrap();

    let polled = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&[PromiseStateId(11)]).unwrap())
        .await;
    let Err(error) = polled else {
        panic!("a failed pass must fail the wait");
    };
    assert!(matches!(error, Error::Lock(_)));
    assert!(h.polls.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn corrupt_stored_payload_fails_the_wait_and_unlocks() {
    let (mut h, requester) = harness();
    h.backend.rows.lock().unwrap().insert(
        key(11),
        MockRow {
            effect_number: EffectNumber(3),
            request: b"not a valid payload".to_vec(),
            locked_by: None,
            lock_expires_at: None,
            lock_nonce: None,
        },
    );
    h.settlements_tx.send(settlement(11)).unwrap();

    let polled = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&[PromiseStateId(11)]).unwrap())
        .await;
    let Err(error) = polled else {
        panic!("a corrupt payload must fail the wait");
    };
    assert!(matches!(error, Error::Decode(_)));
    assert!(requester.requests.lock().unwrap().is_empty());
    assert!(h.polls.lock().unwrap().is_empty());

    // The lock taken for the call that never went through is up for
    // unlocking.
    let tracker = h.tracker.lock().unwrap();
    assert!(!tracks_request(&tracker, &key(11)));
    assert!(tracker.pending_unlocks().is_some());
}

#[tokio::test(start_paused = true)]
async fn redelivery_failure_fails_the_wait_and_unlocks() {
    let mut h = harness_with(FailingRequester);
    seed_row(
        &h.backend,
        42,
        11,
        3,
        vec![1i64],
        Some(99),
        Duration::minutes(-5),
    );
    h.settlements_tx.send(settlement(11)).unwrap();

    let polled = h
        .settler
        .poll_action_settlements(NESlice::try_from_slice(&[PromiseStateId(11)]).unwrap())
        .await;
    let Err(error) = polled else {
        panic!("a failed redelivery must fail the wait");
    };
    assert!(matches!(error, Error::Deliver(_)));
    assert!(h.polls.lock().unwrap().is_empty());

    let tracker = h.tracker.lock().unwrap();
    assert!(!tracks_request(&tracker, &key(11)));
    let pending = tracker
        .pending_unlocks()
        .expect("the lock is up for unlocking");
    assert_eq!(
        pending
            .lock_ids()
            .iter()
            .map(|lock_id| lock_id.request)
            .collect::<Vec<_>>(),
        vec![key(11)]
    );
}

/// Poll `future` once the runtime is idle; true while it is still pending.
async fn poll_once_is_pending<Fut>(future: &mut std::pin::Pin<&mut Fut>) -> bool
where
    Fut: Future,
{
    // Let the lock batcher flush and answer before the future is polled.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut polled = std::future::poll_fn(|context| {
        std::task::Poll::Ready(future.as_mut().poll(context).is_pending())
    });
    std::pin::Pin::new(&mut polled).await
}
