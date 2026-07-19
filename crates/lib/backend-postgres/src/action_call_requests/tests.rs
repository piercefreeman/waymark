use chrono::{Duration, Utc};
use nonempty_collections::NEVec;
use serial_test::serial;
use waymark_action_completions_reconciler_backend::{CompletionRecord, RecordCompletions as _};
use waymark_action_effect_reconciler_backend::record_action_call_requests;
use waymark_action_effect_reconciler_backend::record_action_call_requests::Error as _;
use waymark_action_effect_reconciler_backend::renew_action_call_request_locks::RenewalStatus;
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestRecord, LockVmActionCallRequests as _,
    RecordActionCallRequests as _, RenewActionCallRequestLocks as _, RequestLock,
    UnlockActionCallRequests as _,
};
use waymark_ids::InstanceId;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::super::test_helpers::{register_test_vm, setup_backend};
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
        .record_action_call_requests(live_lock(owner), records.as_nonempty_slice())
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
        .record_action_call_requests(live_lock(uuid::Uuid::new_v4()), replay.as_nonempty_slice())
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
        .record_action_call_requests(live_lock(uuid::Uuid::new_v4()), records.as_nonempty_slice())
        .await
        .expect("fresh record");

    let divergent = NEVec::try_from_vec(vec![record(vm, 1, 10, b"DIFFERENT")]).unwrap();
    let error = backend
        .record_action_call_requests(
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
        .record_action_call_requests(expired_lock(dead_owner), expired.as_nonempty_slice())
        .await
        .expect("record expired-locked");
    let live = NEVec::try_from_vec(vec![record(vm, 2, 11, b"live-call")]).unwrap();
    backend
        .record_action_call_requests(live_lock(live_owner), live.as_nonempty_slice())
        .await
        .expect("record live-locked");

    let outcome = backend
        .lock_vm_action_call_requests(Utc::now(), live_lock(reviver), &vm)
        .await
        .expect("lock vm requests");

    assert_eq!(outcome.locked.len(), 1);
    assert_eq!(outcome.locked[0], record(vm, 1, 10, b"expired-call"));
    assert_eq!(outcome.held_elsewhere, vec![key(vm, 2)]);

    // A VM with no rows is a no-op.
    let other_vm = InstanceId::new_uuid_v4();
    let outcome = backend
        .lock_vm_action_call_requests(Utc::now(), live_lock(reviver), &other_vm)
        .await
        .expect("lock empty vm");
    assert!(outcome.locked.is_empty());
    assert!(outcome.held_elsewhere.is_empty());
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
        .record_action_call_requests(live_lock(owner), mine.as_nonempty_slice())
        .await
        .expect("record mine");
    let theirs = NEVec::try_from_vec(vec![record(vm, 2, 11, b"theirs")]).unwrap();
    backend
        .record_action_call_requests(live_lock(other), theirs.as_nonempty_slice())
        .await
        .expect("record theirs");

    let statuses = backend
        .renew_action_call_request_locks(
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

#[serial(postgres)]
#[tokio::test]
async fn unlock_releases_own_locks_only() {
    let backend = setup_backend().await;
    let vm = InstanceId::new_uuid_v4();
    let owner = uuid::Uuid::new_v4();
    let other = uuid::Uuid::new_v4();

    let mine = NEVec::try_from_vec(vec![record(vm, 1, 10, b"mine")]).unwrap();
    backend
        .record_action_call_requests(live_lock(owner), mine.as_nonempty_slice())
        .await
        .expect("record mine");
    let theirs = NEVec::try_from_vec(vec![record(vm, 2, 11, b"theirs")]).unwrap();
    backend
        .record_action_call_requests(live_lock(other), theirs.as_nonempty_slice())
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
    let outcome = backend
        .lock_vm_action_call_requests(Utc::now(), live_lock(reviver), &vm)
        .await
        .expect("lock vm requests");
    assert_eq!(outcome.locked.len(), 1);
    assert_eq!(outcome.locked[0], record(vm, 1, 10, b"mine"));
    assert_eq!(outcome.held_elsewhere, vec![key(vm, 2)]);
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
        .record_action_call_requests(live_lock(owner), requests.as_nonempty_slice())
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
        .record_action_call_requests(live_lock(owner), records.as_nonempty_slice())
        .await
        .expect("record");

    sqlx::query("DELETE FROM vm_runtime_snapshots WHERE vm_id = $1")
        .bind(vm_a)
        .execute(backend.pool())
        .await
        .expect("delete vm_a snapshot");

    let statuses = backend
        .renew_action_call_request_locks(
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
