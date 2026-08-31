use chrono::{Duration, Utc};
use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _, nev};
use serial_test::serial;
use waymark_action_completions_reconciler_backend::{CompletionRecord, RecordCompletions as _};
use waymark_action_effect_reconciler_backend::record_action_call_requests;
use waymark_action_effect_reconciler_backend::record_action_call_requests::Error as _;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::RenewalStatus;
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestRecord, LockActionCallRequests as _,
    RecordActionCallRequests as _, RenewActionCallRequestLocks as _, RequestLock,
    UnlockActionCallRequests as _,
};
use waymark_ids::InstanceId;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::super::test_helpers::{
    deadlock, register_test_vm, register_test_vm_with_id, setup_backend,
};
use super::error::RecordError;

fn record(
    vm_id: InstanceId,
    promise: usize,
    effect: usize,
    request: &[u8],
) -> ActionCallRequestRecord<InstanceId> {
    ActionCallRequestRecord {
        vm_id,
        promise_state_id: PromiseStateId(promise),
        effect_number: EffectNumber(effect),
        request: request.to_vec(),
    }
}

fn key(vm_id: InstanceId, promise: usize) -> ActionCallRequestKey<InstanceId> {
    ActionCallRequestKey {
        vm_id,
        promise_state_id: PromiseStateId(promise),
    }
}

fn live_lock(owner: uuid::Uuid) -> RequestLock<uuid::Uuid, chrono::DateTime<Utc>> {
    RequestLock {
        owner,
        expires_at: Utc::now() + Duration::minutes(5),
    }
}

fn expired_lock(owner: uuid::Uuid) -> RequestLock<uuid::Uuid, chrono::DateTime<Utc>> {
    RequestLock {
        owner,
        expires_at: Utc::now() - Duration::minutes(5),
    }
}

#[serial(postgres)]
#[tokio::test]
async fn record_fresh_then_identical_replay() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();

    let records = nev![record(vm, 1, 10, b"call-1"), record(vm, 2, 11, b"call-2")];

    let success = backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("fresh record");
    assert_eq!(
        success,
        record_action_call_requests::RecordingSuccess::AllRecorded
    );

    // A replayed effect re-inserts byte-identical rows: idempotently
    // accepted, reported, and the rows stay untouched.
    let replay = nev![record(vm, 1, 10, b"call-1")];
    let success = backend
        .record_action_call_requests(
            Utc::now(),
            live_lock(uuid::Uuid::new_v4()),
            replay.as_nonempty_slice(),
        )
        .await
        .expect("replayed record");
    assert_eq!(
        success,
        record_action_call_requests::RecordingSuccess::SomeAlreadyRecorded(nev![key(vm, 1)])
    );

    // Untouched includes the lock: the original owner still renews.
    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            nev![key(vm, 1), key(vm, 2)].as_nonempty_slice(),
        )
        .await
        .expect("renew");
    assert!(
        statuses
            .iter()
            .all(|renewal| renewal.status == RenewalStatus::Renewed)
    );
}

#[serial(postgres)]
#[tokio::test]
async fn record_divergent_payload_fails_loudly() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();

    let records = nev![record(vm, 1, 10, b"original")];
    backend
        .record_action_call_requests(
            Utc::now(),
            live_lock(uuid::Uuid::new_v4()),
            records.as_nonempty_slice(),
        )
        .await
        .expect("fresh record");

    let divergent = nev![record(vm, 1, 10, b"DIFFERENT")];
    let error = backend
        .record_action_call_requests(
            Utc::now(),
            live_lock(uuid::Uuid::new_v4()),
            divergent.as_nonempty_slice(),
        )
        .await
        .expect_err("divergent payload must fail");
    let RecordError::DivergentPayload(keys) = &error else {
        panic!("expected DivergentPayload, got {error:?}");
    };
    assert_eq!(
        keys.clone().into_iter().collect::<Vec<_>>(),
        vec![key(vm, 1)]
    );
    assert_eq!(
        error.kind(),
        record_action_call_requests::ErrorKind::DivergentPayload
    );

    // A diverging effect number alone is the same violation.
    let divergent = nev![record(vm, 1, 99, b"original")];
    let error = backend
        .record_action_call_requests(
            Utc::now(),
            live_lock(uuid::Uuid::new_v4()),
            divergent.as_nonempty_slice(),
        )
        .await
        .expect_err("divergent effect number must fail");
    assert!(matches!(error, RecordError::DivergentPayload(_)));
}

#[serial(postgres)]
#[tokio::test]
async fn lock_vm_takes_expired_and_reports_foreign() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let dead_owner = uuid::Uuid::new_v4();
    let live_owner = uuid::Uuid::new_v4();
    let reviver = uuid::Uuid::new_v4();

    // One request whose lock expired (dead process), one still held live.
    let expired = nev![record(vm, 1, 10, b"expired-call")];
    backend
        .record_action_call_requests(
            Utc::now(),
            expired_lock(dead_owner),
            expired.as_nonempty_slice(),
        )
        .await
        .expect("record expired-locked");
    let live = nev![record(vm, 2, 11, b"live-call")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(live_owner), live.as_nonempty_slice())
        .await
        .expect("record live-locked");

    // One batch spanning the seeded VM and an empty one: per-input-aligned
    // outcomes, with the empty VM keeping its (no-op) entry.
    let other_vm = InstanceId::new_uuid_v4();
    let vm_ids = [vm, other_vm];
    let outcomes = backend
        .lock_action_call_requests(
            Utc::now(),
            live_lock(reviver),
            nonempty_collections::NESlice::try_from_slice(&vm_ids).unwrap(),
        )
        .await
        .expect("lock vm requests");

    assert_eq!(outcomes.len().get(), 2);
    assert_eq!(outcomes[0].vm_id, vm);
    assert_eq!(outcomes[0].locked.len(), 1);
    assert_eq!(outcomes[0].locked[0], record(vm, 1, 10, b"expired-call"));
    assert_eq!(outcomes[0].held_elsewhere, vec![key(vm, 2)]);

    assert_eq!(outcomes[1].vm_id, other_vm);
    assert!(outcomes[1].locked.is_empty());
    assert!(outcomes[1].held_elsewhere.is_empty());
}

#[serial(postgres)]
#[tokio::test]
async fn renew_reports_per_key_status() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();
    let other = uuid::Uuid::new_v4();

    let mine = nev![record(vm, 1, 10, b"mine")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), mine.as_nonempty_slice())
        .await
        .expect("record mine");
    let theirs = nev![record(vm, 2, 11, b"theirs")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(other), theirs.as_nonempty_slice())
        .await
        .expect("record theirs");

    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            nev![key(vm, 1), key(vm, 2), key(vm, 3)].as_nonempty_slice(),
        )
        .await
        .expect("renew");

    let status_of = |promise: usize| {
        statuses
            .iter()
            .find(|renewal| renewal.key == key(vm, promise))
            .expect("status present")
            .status
    };
    assert_eq!(status_of(1), RenewalStatus::Renewed);
    assert_eq!(status_of(2), RenewalStatus::HeldElsewhere);
    assert_eq!(status_of(3), RenewalStatus::Missing);
}

/// A completion removing the request row while the renewal statement is
/// already running must classify as [`RenewalStatus::Missing`]: the
/// renewal statement's snapshot still shows the row, and concluding
/// "held elsewhere" from that stale read would breach the fence over a
/// successfully completed call.
#[serial(postgres)]
#[tokio::test]
async fn renew_racing_a_row_removal_reports_missing() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();

    let records = nev![record(vm, 1, 10, b"racing")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record request");

    // Hold the row lock with an uncommitted removal — standing in for the
    // completion trigger's DELETE — so the renewal statement blocks on the
    // row while its snapshot predates the removal.
    let mut removal_transaction = backend.pool().begin().await.expect("begin removal");
    sqlx::query("DELETE FROM action_call_requests WHERE vm_id = $1 AND promise_state_id = $2")
        .bind(vm)
        .bind(1i64)
        .execute(&mut *removal_transaction)
        .await
        .expect("delete request row");

    let renew_backend = backend.clone();
    let renew_task = tokio::spawn(async move {
        renew_backend
            .renew_action_call_request_locks(
                Utc::now(),
                live_lock(owner),
                nev![key(vm, 1)].as_nonempty_slice(),
            )
            .await
    });

    // Let the renewal reach the locked row, then let the removal win.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    removal_transaction.commit().await.expect("commit removal");

    let statuses = renew_task.await.expect("join renew").expect("renew");
    assert_eq!(statuses.len().get(), 1);
    assert_eq!(statuses.first().status, RenewalStatus::Missing);
}

#[serial(postgres)]
#[tokio::test]
async fn unlock_releases_own_locks_only() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();
    let other = uuid::Uuid::new_v4();

    let mine = nev![record(vm, 1, 10, b"mine")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), mine.as_nonempty_slice())
        .await
        .expect("record mine");
    let theirs = nev![record(vm, 2, 11, b"theirs")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(other), theirs.as_nonempty_slice())
        .await
        .expect("record theirs");

    backend
        .unlock_action_call_requests(&owner, nev![key(vm, 1), key(vm, 2)].as_nonempty_slice())
        .await
        .expect("unlock");

    // The unlocked row is immediately deliverable; the foreign one is not.
    let reviver = uuid::Uuid::new_v4();
    let outcomes = backend
        .lock_action_call_requests(
            Utc::now(),
            live_lock(reviver),
            nonempty_collections::NESlice::try_from_slice(std::slice::from_ref(&vm)).unwrap(),
        )
        .await
        .expect("lock vm requests");
    assert_eq!(outcomes[0].locked.len(), 1);
    assert_eq!(outcomes[0].locked[0], record(vm, 1, 10, b"mine"));
    assert_eq!(outcomes[0].held_elsewhere, vec![key(vm, 2)]);
}

#[serial(postgres)]
#[tokio::test]
async fn recording_a_completion_removes_the_request() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();

    let requests = nev![record(vm, 1, 10, b"call-1"), record(vm, 2, 11, b"call-2")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), requests.as_nonempty_slice())
        .await
        .expect("record requests");

    // Recording the completion for promise 1 removes exactly its request
    // row, atomically, via the schema trigger.
    let completions = nev![CompletionRecord {
        vm_id: vm,
        promise_state_id: PromiseStateId(1),
        effect_number: EffectNumber(10),
        outcome: b"outcome-1".to_vec(),
    }];
    backend
        .record_completions(completions.as_nonempty_slice())
        .await
        .expect("record completion");

    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            nev![key(vm, 1), key(vm, 2)].as_nonempty_slice(),
        )
        .await
        .expect("renew");
    let status_of = |promise: usize| {
        statuses
            .iter()
            .find(|renewal| renewal.key == key(vm, promise))
            .expect("status present")
            .status
    };
    assert_eq!(status_of(1), RenewalStatus::Missing);
    assert_eq!(status_of(2), RenewalStatus::Renewed);

    // A redelivered (conflict-skipped) completion does not fire the
    // trigger again — nothing left to remove, and no error.
    backend
        .record_completions(completions.as_nonempty_slice())
        .await
        .expect("redeliver completion");
}

#[serial(postgres)]
#[tokio::test]
async fn snapshot_delete_sweeps_only_the_vms_requests() {
    let backend = setup_backend().await;
    let (vm_a, _) = register_test_vm(&backend).await;
    let (vm_b, _) = register_test_vm(&backend).await;
    let owner = uuid::Uuid::new_v4();

    let records = nev![record(vm_a, 1, 10, b"a1"), record(vm_b, 1, 20, b"b1")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record");

    sqlx::query("DELETE FROM vm_runtime_snapshots WHERE vm_id = $1")
        .bind(vm_a)
        .execute(backend.pool())
        .await
        .expect("delete vm_a snapshot");

    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            nev![key(vm_a, 1), key(vm_b, 1)].as_nonempty_slice(),
        )
        .await
        .expect("renew");
    let status_of = |vm: InstanceId, promise: usize| {
        statuses
            .iter()
            .find(|renewal| renewal.key == key(vm, promise))
            .expect("status present")
            .status
    };
    assert_eq!(status_of(vm_a, 1), RenewalStatus::Missing);
    assert_eq!(status_of(vm_b, 1), RenewalStatus::Renewed);
}

/// One choreographed collision of the completion trigger's DELETE and
/// the renewal UPDATE over the same two request rows.
///
/// Both statements take their row locks in input order — the trigger in
/// transition-table (insert) order, the renewal in input-array order,
/// verified empirically against the real plans at this scale — and
/// neither imposes a canonical order, so opposite input orders form a
/// deadlock cycle.  The staging makes the cycle deterministic instead of
/// probabilistic: a helper transaction pre-holds the completion batch's
/// FIRST row, which is the renewal batch's LAST row.  The completion
/// blocks on it holding nothing; the renewal then locks every other row
/// and queues behind the completion.  Releasing the staged row lets the
/// completion advance into the renewal's rows while the renewal waits on
/// the completion's — the deadlock, detected by Postgres within
/// `deadlock_timeout`.
///
/// This is the production outage shape (2026-08-31): batched lock
/// renewals and batched completion inserts deadlocking on the same
/// `action_call_requests` rows, at batch sizes where overlap in
/// conflicting orders is routine.  The test's contract is simply that
/// both statements succeed: any ordering discipline that makes the
/// deadlock impossible passes it, and disciplining only one side does
/// not.
async fn complete_and_renew_across_a_staged_row(completion_order: [usize; 2]) {
    // The renewal walks the same rows in the opposite order, making the
    // staged row the completion's first and the renewal's last.
    let renewal_order = [completion_order[1], completion_order[0]];

    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();

    let records = nev![record(vm, 1, 10, b"call-1"), record(vm, 2, 11, b"call-2")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record requests");

    deadlock::seed_filler_rows(backend.pool(), deadlock::SweptTable::ActionCallRequests).await;

    // Pre-hold the row where the completion enters and the renewal exits.
    let staged = completion_order[0];
    let staging = deadlock::hold_row_for_update(
        backend.pool(),
        deadlock::SweptTable::ActionCallRequests,
        vm,
        staged,
    )
    .await;

    // The completion batch first: it must be the head of the staged
    // row's wait queue, so releasing hands the row to it and not to the
    // renewal queued second.
    let completion_backend = backend.clone();
    let completion_task = tokio::spawn(async move {
        let completions = NEVec::try_from_vec(
            completion_order
                .iter()
                .map(|&promise| CompletionRecord {
                    vm_id: vm,
                    promise_state_id: PromiseStateId(promise),
                    effect_number: EffectNumber(promise + 9),
                    outcome: b"done".to_vec(),
                })
                .collect(),
        )
        .unwrap();
        completion_backend
            .record_completions(completions.as_nonempty_slice())
            .await
    });
    deadlock::wait_until_lock_blocked(backend.pool(), "%INSERT INTO action_call_completions%")
        .await;

    let renewal_backend = backend.clone();
    let renewal_task = tokio::spawn(async move {
        let keys: NEVec<_> = renewal_order
            .into_nonempty_iter()
            .map(|promise| key(vm, promise))
            .collect();
        renewal_backend
            .renew_action_call_request_locks(Utc::now(), live_lock(owner), keys.as_nonempty_slice())
            .await
    });
    deadlock::wait_until_lock_blocked(backend.pool(), "%WITH input(vm_id, promise_state_id)%")
        .await;

    staging.rollback().await.expect("release staged row");

    completion_task
        .await
        .expect("join completion")
        .expect("recording completions must not deadlock against lock renewal");
    renewal_task
        .await
        .expect("join renewal")
        .expect("renewing locks must not deadlock against completion recording");
}

#[serial(postgres)]
#[tokio::test]
async fn concurrent_completion_and_renewal_batches_do_not_deadlock() {
    // Both mirrored input orders.  This pins the outage pair end to end;
    // note the completion path is pinned as a whole — its two mechanisms
    // (the INSERT's ORDER BY canonicalizing the transition table, the
    // trigger's ordered lock pass) are mutually redundant here, so
    // removing either one alone stays green.  The trigger body itself is
    // pinned by the multi-VM sweep test below, and the INSERT's ORDER BY
    // by the heap-order test below.
    complete_and_renew_across_a_staged_row([2, 1]).await;
    complete_and_renew_across_a_staged_row([1, 2]).await;
}

/// The individual pin on the batch INSERT's ORDER BY, whose independent
/// job is canonicalizing conflict-arbiter waits between two overlapping
/// replayed batches (an `ORDER BY` feeding `ON CONFLICT DO NOTHING`
/// looks removable without it).  The arbiter cycle itself cannot be
/// staged deterministically — waiters on an aborted speculative insert
/// wake in no particular order, so a choreographed release resolves by
/// coin flip — so this pins the mechanism directly instead: rows land in
/// the heap in insertion order, `ctid` exposes that order, and a batch
/// submitted out of order must still land in primary-key order.
#[serial(postgres)]
#[tokio::test]
async fn record_batch_inserts_in_primary_key_order() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();

    let records = nev![
        record(vm, 2, 11, b"call-2"),
        record(vm, 3, 12, b"call-3"),
        record(vm, 1, 10, b"call-1")
    ];
    backend
        .record_action_call_requests(
            Utc::now(),
            live_lock(uuid::Uuid::new_v4()),
            records.as_nonempty_slice(),
        )
        .await
        .expect("record requests");

    let heap_order: Vec<i64> = sqlx::query_scalar(
        r#"
        SELECT promise_state_id FROM action_call_requests
        WHERE vm_id = $1 ORDER BY ctid
        "#,
    )
    .bind(vm)
    .fetch_all(backend.pool())
    .await
    .expect("read heap order");
    assert_eq!(
        heap_order,
        [1, 2, 3],
        "the batch INSERT no longer writes in primary-key order — \
         conflict-arbiter waits between overlapping replayed batches \
         are back to input order and can cycle"
    );
}

/// Premise canary for the UNNEST-join plan shape the op-side
/// choreographies rely on: two hand-written PRE-discipline statements —
/// plain input-order UNNEST-join UPDATE statements with opposite key
/// orders — must still deadlock on this planner and schema.  The
/// do-not-deadlock tests can only detect a lost discipline while the
/// planner drives such statements' locks in input order; if plan shapes
/// ever drift (a hash join over a seq scan aligns both sides' lock
/// orders), this test goes red instead of those tests going silently
/// vacuous.  The multi-VM sweep test guards its own, separate heap-order
/// premise with an in-test plan assertion.
#[serial(postgres)]
#[tokio::test]
async fn undisciplined_writers_still_deadlock_premise_canary() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();

    let records = nev![record(vm, 1, 10, b"call-1"), record(vm, 2, 11, b"call-2")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record requests");

    deadlock::seed_filler_rows(backend.pool(), deadlock::SweptTable::ActionCallRequests).await;

    let staging = deadlock::hold_row_for_update(
        backend.pool(),
        deadlock::SweptTable::ActionCallRequests,
        vm,
        2,
    )
    .await;

    let undisciplined_update = |tag: &'static str, order: [i64; 2]| {
        let pool = backend.pool().clone();
        tokio::spawn(async move {
            sqlx::query(&format!(
                r#"
                /* premise canary {tag} */
                UPDATE action_call_requests r
                SET lock_expires_at = NOW() + interval '1 minute'
                FROM UNNEST($1::uuid[], $2::bigint[]) AS i(vm_id, promise_state_id)
                WHERE r.vm_id = i.vm_id AND r.promise_state_id = i.promise_state_id
                "#
            ))
            .bind(vec![vm, vm])
            .bind(order.to_vec())
            .execute(&pool)
            .await
        })
    };

    // Descending first: it blocks on the staged row holding nothing, the
    // ascending one locks row 1 and queues behind it — releasing forms
    // the cycle.
    let descending = undisciplined_update("descending", [2, 1]);
    deadlock::wait_until_lock_blocked(backend.pool(), "%premise canary descending%").await;
    let ascending = undisciplined_update("ascending", [1, 2]);
    deadlock::wait_until_lock_blocked(backend.pool(), "%premise canary ascending%").await;

    staging.rollback().await.expect("release staged row");

    let results = [
        descending.await.expect("join descending"),
        ascending.await.expect("join ascending"),
    ];
    let deadlocked = results
        .iter()
        .filter(|result| {
            result.as_ref().is_err_and(|error| {
                error
                    .as_database_error()
                    .is_some_and(|db| db.code().as_deref() == Some("40P01"))
            })
        })
        .count();
    assert_eq!(
        deadlocked, 1,
        "input-order statements no longer deadlock — plan shapes drifted and the \
         do-not-deadlock choreographies in this suite may be vacuous: {results:?}"
    );
}

/// One choreographed collision of the renewal UPDATE and the
/// snapshot-cleanup trigger's requests sweep over the same two request
/// rows — the requests-table sweep pairing of the multi-row-writer
/// class behind the 2026-08-31 deadlock outage (renewing a VM's locks
/// while its terminal delete sweeps them).
///
/// The staging, plan-shape seeding, and contract are those of
/// `complete_and_renew_across_a_staged_row` above; the sweep's lock
/// order is not input-driven — it walks the whole VM — so the mirroring
/// axis is the renewal batch's order plus which row is staged.
async fn renew_and_sweep_across_a_staged_row(renewal_order: [usize; 2]) {
    let backend = setup_backend().await;
    let (vm, _) = register_test_vm(&backend).await;
    let owner = uuid::Uuid::new_v4();

    let records = nev![record(vm, 1, 10, b"call-1"), record(vm, 2, 11, b"call-2")];
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record requests");

    deadlock::seed_filler_rows(backend.pool(), deadlock::SweptTable::ActionCallRequests).await;

    // Pre-hold the renewal batch's first row.
    let staged = renewal_order[0];
    let staging = deadlock::hold_row_for_update(
        backend.pool(),
        deadlock::SweptTable::ActionCallRequests,
        vm,
        staged,
    )
    .await;

    // The renewal first: it must be the head of the staged row's wait
    // queue, so releasing hands the row to it and not to the sweep.
    let renewal_backend = backend.clone();
    let renewal_task = tokio::spawn(async move {
        let keys: NEVec<_> = renewal_order
            .into_nonempty_iter()
            .map(|promise| key(vm, promise))
            .collect();
        renewal_backend
            .renew_action_call_request_locks(Utc::now(), live_lock(owner), keys.as_nonempty_slice())
            .await
    });
    deadlock::contend_op_with_snapshot_sweep(
        backend.pool(),
        staging,
        "renewal",
        renewal_task,
        "%WITH input(vm_id, promise_state_id)%",
        vm,
    )
    .await;
}

#[serial(postgres)]
#[tokio::test]
async fn concurrent_renewal_and_snapshot_sweep_do_not_deadlock() {
    // Both renewal orders, with the staged row tracking the renewal's
    // first key.  These runs pin the RENEWAL side's discipline only: a
    // single-VM sweep locks its rows in primary-key order under every
    // realizable plan (index probe and fresh-heap seq scan alike), so
    // the trigger side cannot regress visibly here — the multi-VM sweep
    // test below covers it.
    renew_and_sweep_across_a_staged_row([2, 1]).await;
    renew_and_sweep_across_a_staged_row([1, 2]).await;
}

/// The multi-VM sweep choreography — the only shape that exercises the
/// snapshot-cleanup trigger's OWN lock order.  A two-VM snapshot delete
/// hands the trigger's requests pass rows spanning two VMs in heap
/// (registration) order — arranged DESCENDING by vm_id below, so a
/// trigger without the ordered lock pass provably inverts primary-key
/// order — contended against the canonically-ordered revival lock.  No
/// production multi-row snapshot delete exists yet, but 0020's design
/// anticipates them; this pins the trigger body for that day.
///
/// `sweep_first` mirrors which statement leads: the leader's first
/// contended row is staged so it blocks holding nothing, and the
/// follower locks the other row and queues behind it — a discipline
/// missing from the trigger fails the sweep-first run, one missing from
/// the revival lock fails the other.
async fn multi_vm_sweep_and_revival_lock_across_a_staged_row(sweep_first: bool) {
    let backend = setup_backend().await;

    // The snapshots table is tiny and fresh, so the sweep's scan order
    // is heap order, which is registration order: mint the ids up
    // front and register the HIGHER one first, so the sweep provably
    // walks in descending vm_id order.
    let mut vm_ids = [InstanceId::new_uuid_v4(), InstanceId::new_uuid_v4()];
    vm_ids.sort();
    let [vm_lo, vm_hi] = vm_ids;
    register_test_vm_with_id(&backend, vm_hi).await;
    register_test_vm_with_id(&backend, vm_lo).await;

    // One expired-locked request row per VM, so the revival lock is
    // eligible to take both.
    let dead_owner = uuid::Uuid::new_v4();
    let records = nev![record(vm_lo, 1, 10, b"lo"), record(vm_hi, 1, 11, b"hi")];
    backend
        .record_action_call_requests(
            Utc::now(),
            expired_lock(dead_owner),
            records.as_nonempty_slice(),
        )
        .await
        .expect("record requests");

    deadlock::seed_filler_rows(backend.pool(), deadlock::SweptTable::ActionCallRequests).await;

    // Premise guard: the descending arrangement above reaches the
    // trigger only through a heap-order scan of the snapshot delete
    // (seq or bitmap — both preserve heap order).  A plain index scan
    // would hand the trigger ascending rows regardless, quietly
    // blunting this test's only pin on the trigger body — fail here
    // instead of passing vacuously.
    let sweep_plan: Vec<String> =
        sqlx::query_scalar("EXPLAIN DELETE FROM vm_runtime_snapshots WHERE vm_id = ANY($1)")
            .bind(vec![vm_hi, vm_lo])
            .fetch_all(backend.pool())
            .await
            .expect("explain the sweep");
    assert!(
        !sweep_plan
            .iter()
            .any(|line| line.contains("Index Scan using")),
        "the snapshot sweep plans as a plain index scan, which emits \
         ascending key order regardless of heap order — the sweep-first \
         run can no longer detect a trigger-side regression: {sweep_plan:?}"
    );

    // The staged row is the leader's first contact: vm_hi's request row
    // (heap-first for the sweep, input-first for the revival lock).
    let staging = deadlock::hold_row_for_update(
        backend.pool(),
        deadlock::SweptTable::ActionCallRequests,
        vm_hi,
        1,
    )
    .await;

    let spawn_revival_lock = |vm_ids: Vec<InstanceId>| {
        let backend = backend.clone();
        tokio::spawn(async move {
            let vm_ids = NEVec::try_from_vec(vm_ids).unwrap();
            backend
                .lock_action_call_requests(
                    Utc::now(),
                    live_lock(uuid::Uuid::new_v4()),
                    vm_ids.as_nonempty_slice(),
                )
                .await
        })
    };
    let revival_pattern = "%locked_by IS NULL OR%";

    if sweep_first {
        let sweep = deadlock::spawn_snapshot_sweep(backend.pool(), vec![vm_hi, vm_lo]);
        deadlock::wait_until_lock_blocked(backend.pool(), deadlock::SNAPSHOT_SWEEP_PATTERN).await;
        let revival = spawn_revival_lock(vec![vm_lo, vm_hi]);
        deadlock::wait_until_lock_blocked(backend.pool(), revival_pattern).await;

        staging.rollback().await.expect("release staged row");

        sweep
            .await
            .expect("join sweep")
            .expect("the snapshot sweep must not deadlock against the revival lock");
        revival
            .await
            .expect("join revival")
            .expect("the revival lock must not deadlock against the snapshot sweep");
    } else {
        let revival = spawn_revival_lock(vec![vm_hi, vm_lo]);
        deadlock::wait_until_lock_blocked(backend.pool(), revival_pattern).await;
        let sweep = deadlock::spawn_snapshot_sweep(backend.pool(), vec![vm_lo, vm_hi]);
        deadlock::wait_until_lock_blocked(backend.pool(), deadlock::SNAPSHOT_SWEEP_PATTERN).await;

        staging.rollback().await.expect("release staged row");

        revival
            .await
            .expect("join revival")
            .expect("the revival lock must not deadlock against the snapshot sweep");
        sweep
            .await
            .expect("join sweep")
            .expect("the snapshot sweep must not deadlock against the revival lock");
    }
}

#[serial(postgres)]
#[tokio::test]
async fn concurrent_multi_vm_sweep_and_revival_lock_do_not_deadlock() {
    // Leader mirrored both ways: sweep-first pins the trigger's ordered
    // lock pass, revival-first pins the revival lock's.
    multi_vm_sweep_and_revival_lock_across_a_staged_row(true).await;
    multi_vm_sweep_and_revival_lock_across_a_staged_row(false).await;
}
