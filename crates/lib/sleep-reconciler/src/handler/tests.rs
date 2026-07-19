use std::sync::Arc;
use std::time::Duration;

use waymark_extcall_reconciler_core::SleepEffectHandler as _;
use waymark_ids::InstanceId;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_sleep_reconciler_backend::record_sleeps;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::EffectHandler;
use crate::test_support::{MockBackend, MockRecordError};

#[tokio::test]
async fn records_the_absolute_wake_deadline() {
    let backend = MockBackend::default();
    let vm_id = InstanceId::new_uuid_v4();
    let mut handler = EffectHandler {
        backend: Arc::new(backend.clone()),
        vm_id,
    };

    let duration = NonZeroDuration::try_from(Duration::from_secs(60)).unwrap();
    let before = chrono::Utc::now();
    handler
        .record_sleep(EffectNumber(7), PromiseStateId(3), duration)
        .await
        .expect("record");
    let after = chrono::Utc::now();

    let recorded = backend.inner.recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    let record = &recorded[0];
    assert_eq!(record.vm_id, vm_id);
    assert_eq!(record.promise_state_id, PromiseStateId(3));
    assert_eq!(record.effect_number, EffectNumber(7));
    assert!(record.wake_at >= before + chrono::Duration::seconds(60));
    assert!(record.wake_at <= after + chrono::Duration::seconds(60));
}

#[tokio::test]
async fn record_errors_propagate() {
    let backend = MockBackend::default();
    backend
        .inner
        .record_responses
        .lock()
        .unwrap()
        .push_back(Err(MockRecordError {
            kind: record_sleeps::ErrorKind::Internal,
        }));
    let mut handler = EffectHandler {
        backend: Arc::new(backend.clone()),
        vm_id: InstanceId::new_uuid_v4(),
    };

    let duration = NonZeroDuration::try_from(Duration::from_secs(1)).unwrap();
    handler
        .record_sleep(EffectNumber(0), PromiseStateId(0), duration)
        .await
        .expect_err("backend failure surfaces");
    assert!(backend.inner.recorded.lock().unwrap().is_empty());
}
