use chrono::{Duration, Utc};
use nonempty_collections::NEVec;
use serial_test::serial;
use waymark_ids::InstanceId;
use waymark_sleep_reconciler_backend::record_sleeps;
use waymark_sleep_reconciler_backend::record_sleeps::Error as _;
use waymark_sleep_reconciler_backend::{
    AckSleeps as _, PollDueSleeps as _, PurgeVmSleeps as _, RecordSleeps as _, SleepKey,
    SleepRecord,
};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::super::test_helpers::setup_backend;
use super::error::RecordError;

fn record(
    vm_id: InstanceId,
    promise: usize,
    effect: usize,
    wake_at: chrono::DateTime<Utc>,
) -> SleepRecord<InstanceId, chrono::DateTime<Utc>> {
    SleepRecord {
        vm_id,
        promise_state_id: PromiseStateId(promise),
        effect_number: EffectNumber(effect),
        wake_at,
    }
}

fn key(vm_id: InstanceId, promise: usize) -> SleepKey<InstanceId> {
    SleepKey {
        vm_id,
        promise_state_id: PromiseStateId(promise),
    }
}

#[serial(postgres)]
#[tokio::test]
async fn record_then_poll_returns_only_demanded_and_due() {
    let backend = setup_backend().await;
    let vm_a = InstanceId::new_uuid_v4();
    let vm_b = InstanceId::new_uuid_v4();
    let now = Utc::now();
    let past = now - Duration::hours(1);
    let future = now + Duration::hours(1);

    let records = NEVec::try_from_vec(vec![
        record(vm_a, 1, 10, past),
        record(vm_a, 2, 11, future),
        record(vm_b, 1, 20, past),
    ])
    .unwrap();
    backend
        .record_sleeps(records.as_nonempty_slice())
        .await
        .expect("record");

    // Demand vm_a's promises plus one that was never recorded: only the
    // due one comes back — not the future one, not the undemanded vm_b.
    let demand = NEVec::try_from_vec(vec![key(vm_a, 1), key(vm_a, 2), key(vm_a, 99)]).unwrap();
    let polled = backend
        .poll_due_sleeps(now, demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [key(vm_a, 1)]);

    // Once the deadline passes, the future one becomes due as well.
    let polled = backend
        .poll_due_sleeps(future + Duration::seconds(1), demand.as_nonempty_slice())
        .await
        .expect("poll past the deadline");
    assert_eq!(polled.len(), 2);
}

#[serial(postgres)]
#[tokio::test]
async fn re_record_keeps_the_original_wake_at() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let now = Utc::now();
    let original_wake = now - Duration::hours(1);
    let walked_forward_wake = now + Duration::hours(1);

    backend
        .record_sleeps(NEVec::new(record(vm_id, 1, 10, original_wake)).as_nonempty_slice())
        .await
        .expect("first record");

    // A replayed effect recomputes its deadline relative to the replay
    // time; the re-record is silently ignored and the original stands.
    backend
        .record_sleeps(NEVec::new(record(vm_id, 1, 10, walked_forward_wake)).as_nonempty_slice())
        .await
        .expect("re-record is idempotent");

    let demand = NEVec::new(key(vm_id, 1));
    let polled = backend
        .poll_due_sleeps(now, demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [key(vm_id, 1)]);
}

#[serial(postgres)]
#[tokio::test]
async fn record_fails_loudly_on_divergent_effect_number() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let now = Utc::now();
    let wake = now - Duration::hours(1);

    backend
        .record_sleeps(NEVec::new(record(vm_id, 1, 10, wake)).as_nonempty_slice())
        .await
        .expect("first record");

    // Same key, different effect number: broken correlation invariant.
    let error = backend
        .record_sleeps(NEVec::new(record(vm_id, 1, 99, wake)).as_nonempty_slice())
        .await
        .expect_err("divergent effect number must fail");
    assert_eq!(
        error.kind(),
        record_sleeps::ErrorKind::DivergentEffectNumber
    );
    let RecordError::DivergentEffectNumber(keys) = &error else {
        panic!("expected DivergentEffectNumber, got {error:?}");
    };
    assert_eq!(keys, &NEVec::new(key(vm_id, 1)));

    // The stored row is unchanged.
    let polled = backend
        .poll_due_sleeps(now, NEVec::new(key(vm_id, 1)).as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [key(vm_id, 1)]);
}

#[serial(postgres)]
#[tokio::test]
async fn record_accepts_mixed_batch_of_new_and_replayed_rows() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let now = Utc::now();
    let wake = now - Duration::hours(1);

    backend
        .record_sleeps(NEVec::new(record(vm_id, 1, 10, wake)).as_nonempty_slice())
        .await
        .expect("seed record");

    // One replayed duplicate + one new row in the same batch.
    let batch = NEVec::try_from_vec(vec![
        record(vm_id, 1, 10, now + Duration::hours(1)),
        record(vm_id, 2, 11, wake),
    ])
    .unwrap();
    backend
        .record_sleeps(batch.as_nonempty_slice())
        .await
        .expect("mixed batch is accepted");

    let demand = NEVec::try_from_vec(vec![key(vm_id, 1), key(vm_id, 2)]).unwrap();
    let polled = backend
        .poll_due_sleeps(now, demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled.len(), 2);
}

#[serial(postgres)]
#[tokio::test]
async fn ack_removes_rows_and_is_idempotent() {
    let backend = setup_backend().await;
    let vm_id = InstanceId::new_uuid_v4();
    let now = Utc::now();
    let wake = now - Duration::hours(1);

    let records =
        NEVec::try_from_vec(vec![record(vm_id, 1, 10, wake), record(vm_id, 2, 11, wake)]).unwrap();
    backend
        .record_sleeps(records.as_nonempty_slice())
        .await
        .expect("record");

    let acked = NEVec::new(key(vm_id, 1));
    backend
        .ack_sleeps(acked.as_nonempty_slice())
        .await
        .expect("ack");
    // Re-acking (crash recovery) and acking a never-recorded key are no-ops.
    let re_acked = NEVec::try_from_vec(vec![key(vm_id, 1), key(vm_id, 99)]).unwrap();
    backend
        .ack_sleeps(re_acked.as_nonempty_slice())
        .await
        .expect("re-ack is idempotent");

    let demand = NEVec::try_from_vec(vec![key(vm_id, 1), key(vm_id, 2)]).unwrap();
    let polled = backend
        .poll_due_sleeps(now, demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [key(vm_id, 2)]);
}

#[serial(postgres)]
#[tokio::test]
async fn purge_removes_all_rows_of_the_vm_only() {
    let backend = setup_backend().await;
    let vm_a = InstanceId::new_uuid_v4();
    let vm_b = InstanceId::new_uuid_v4();
    let now = Utc::now();
    let wake = now - Duration::hours(1);

    let records = NEVec::try_from_vec(vec![
        record(vm_a, 1, 10, wake),
        record(vm_a, 2, 11, wake),
        record(vm_b, 1, 20, wake),
    ])
    .unwrap();
    backend
        .record_sleeps(records.as_nonempty_slice())
        .await
        .expect("record");

    backend.purge_vm_sleeps(&vm_a).await.expect("purge");
    // Purging a VM with no rows is a no-op.
    backend
        .purge_vm_sleeps(&InstanceId::new_uuid_v4())
        .await
        .expect("purge of unknown vm");

    let demand = NEVec::try_from_vec(vec![key(vm_a, 1), key(vm_a, 2), key(vm_b, 1)]).unwrap();
    let polled = backend
        .poll_due_sleeps(now, demand.as_nonempty_slice())
        .await
        .expect("poll");
    assert_eq!(polled[..], [key(vm_b, 1)]);
}
