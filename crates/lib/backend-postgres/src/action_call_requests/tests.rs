use chrono::{Duration, Utc};
use nonempty_collections::NEVec;
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

use super::super::test_helpers::{register_test_vm, setup_backend, wait_until_lock_blocked};
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

    let records = NEVec::try_from_vec(vec![
        record(vm, 1, 10, b"call-1"),
        record(vm, 2, 11, b"call-2"),
    ])
    .unwrap();

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
    let replay = NEVec::try_from_vec(vec![record(vm, 1, 10, b"call-1")]).unwrap();
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
        record_action_call_requests::RecordingSuccess::SomeAlreadyRecorded(
            NEVec::try_from_vec(vec![key(vm, 1)]).unwrap()
        )
    );

    // Untouched includes the lock: the original owner still renews.
    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            NEVec::try_from_vec(vec![key(vm, 1), key(vm, 2)])
                .unwrap()
                .as_nonempty_slice(),
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

    let records = NEVec::try_from_vec(vec![record(vm, 1, 10, b"original")]).unwrap();
    backend
        .record_action_call_requests(
            Utc::now(),
            live_lock(uuid::Uuid::new_v4()),
            records.as_nonempty_slice(),
        )
        .await
        .expect("fresh record");

    let divergent = NEVec::try_from_vec(vec![record(vm, 1, 10, b"DIFFERENT")]).unwrap();
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
    let divergent = NEVec::try_from_vec(vec![record(vm, 1, 99, b"original")]).unwrap();
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
    let expired = NEVec::try_from_vec(vec![record(vm, 1, 10, b"expired-call")]).unwrap();
    backend
        .record_action_call_requests(
            Utc::now(),
            expired_lock(dead_owner),
            expired.as_nonempty_slice(),
        )
        .await
        .expect("record expired-locked");
    let live = NEVec::try_from_vec(vec![record(vm, 2, 11, b"live-call")]).unwrap();
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

    let mine = NEVec::try_from_vec(vec![record(vm, 1, 10, b"mine")]).unwrap();
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), mine.as_nonempty_slice())
        .await
        .expect("record mine");
    let theirs = NEVec::try_from_vec(vec![record(vm, 2, 11, b"theirs")]).unwrap();
    backend
        .record_action_call_requests(Utc::now(), live_lock(other), theirs.as_nonempty_slice())
        .await
        .expect("record theirs");

    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            NEVec::try_from_vec(vec![key(vm, 1), key(vm, 2), key(vm, 3)])
                .unwrap()
                .as_nonempty_slice(),
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

    let records = NEVec::try_from_vec(vec![record(vm, 1, 10, b"racing")]).unwrap();
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
                NEVec::try_from_vec(vec![key(vm, 1)])
                    .unwrap()
                    .as_nonempty_slice(),
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

    let mine = NEVec::try_from_vec(vec![record(vm, 1, 10, b"mine")]).unwrap();
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), mine.as_nonempty_slice())
        .await
        .expect("record mine");
    let theirs = NEVec::try_from_vec(vec![record(vm, 2, 11, b"theirs")]).unwrap();
    backend
        .record_action_call_requests(Utc::now(), live_lock(other), theirs.as_nonempty_slice())
        .await
        .expect("record theirs");

    backend
        .unlock_action_call_requests(
            &owner,
            NEVec::try_from_vec(vec![key(vm, 1), key(vm, 2)])
                .unwrap()
                .as_nonempty_slice(),
        )
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

    let requests = NEVec::try_from_vec(vec![
        record(vm, 1, 10, b"call-1"),
        record(vm, 2, 11, b"call-2"),
    ])
    .unwrap();
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), requests.as_nonempty_slice())
        .await
        .expect("record requests");

    // Recording the completion for promise 1 removes exactly its request
    // row, atomically, via the schema trigger.
    let completions = NEVec::try_from_vec(vec![CompletionRecord {
        vm_id: vm,
        promise_state_id: PromiseStateId(1),
        effect_number: EffectNumber(10),
        outcome: b"outcome-1".to_vec(),
    }])
    .unwrap();
    backend
        .record_completions(completions.as_nonempty_slice())
        .await
        .expect("record completion");

    let statuses = backend
        .renew_action_call_request_locks(
            Utc::now(),
            live_lock(owner),
            NEVec::try_from_vec(vec![key(vm, 1), key(vm, 2)])
                .unwrap()
                .as_nonempty_slice(),
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

    let records =
        NEVec::try_from_vec(vec![record(vm_a, 1, 10, b"a1"), record(vm_b, 1, 20, b"b1")]).unwrap();
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
            NEVec::try_from_vec(vec![key(vm_a, 1), key(vm_b, 1)])
                .unwrap()
                .as_nonempty_slice(),
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
async fn complete_and_renew_across_a_staged_row(
    completion_order: [usize; 2],
    renewal_order: [usize; 2],
) {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();

    let records = NEVec::try_from_vec(vec![
        record(vm, 1, 10, b"call-1"),
        record(vm, 2, 11, b"call-2"),
    ])
    .unwrap();
    backend
        .record_action_call_requests(Utc::now(), live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record requests");

    // The input-order locking above holds for the production plan shape —
    // small batches probing the primary key of a large table.  Against a
    // freshly-truncated two-row table the planner walks both statements
    // through the same seq scan instead, aligning their lock orders and
    // hiding the bug, so give it the production shape: filler rows and
    // fresh statistics.
    sqlx::query(
        r#"
        INSERT INTO action_call_requests
            (vm_id, promise_state_id, effect_number, request)
        SELECT gen_random_uuid(), n, n, 'filler'
        FROM generate_series(1, 10000) AS n
        "#,
    )
    .execute(backend.pool())
    .await
    .expect("seed filler requests");
    sqlx::query("ANALYZE action_call_requests, action_call_completions")
        .execute(backend.pool())
        .await
        .expect("analyze");

    // Pre-hold the row where the completion enters and the renewal exits.
    let staged = completion_order[0];
    assert_eq!(
        staged, renewal_order[1],
        "the staged row must be the completion's first and the renewal's last"
    );
    let mut staging = backend.pool().begin().await.expect("begin staging");
    sqlx::query(
        r#"
        SELECT 1 FROM action_call_requests
        WHERE vm_id = $1 AND promise_state_id = $2 FOR UPDATE
        "#,
    )
    .bind(vm)
    .bind(i64::try_from(staged).unwrap())
    .execute(&mut *staging)
    .await
    .expect("hold staged row");

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
    wait_until_lock_blocked(backend.pool(), "%INSERT INTO action_call_completions%").await;

    let renewal_backend = backend.clone();
    let renewal_task = tokio::spawn(async move {
        let keys = NEVec::try_from_vec(
            renewal_order
                .iter()
                .map(|&promise| key(vm, promise))
                .collect(),
        )
        .unwrap();
        renewal_backend
            .renew_action_call_request_locks(Utc::now(), live_lock(owner), keys.as_nonempty_slice())
            .await
    });
    wait_until_lock_blocked(backend.pool(), "%UPDATE action_call_requests%").await;

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
    // Both mirrored input orders: each run catches a discipline missing
    // from one of the two statements.
    complete_and_renew_across_a_staged_row([2, 1], [1, 2]).await;
    complete_and_renew_across_a_staged_row([1, 2], [2, 1]).await;
}
