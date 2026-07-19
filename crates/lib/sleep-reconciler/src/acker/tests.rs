use std::sync::Arc;

use waymark_ids::InstanceId;

use crate::test_support::{MockAckError, MockBackend, key};

#[tokio::test]
async fn acks_received_keys_and_stops_when_senders_drop() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    let (tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

    tx.send(key(vm_id, 1)).unwrap();
    tx.send(key(vm_id, 2)).unwrap();
    drop(tx);

    super::run(super::Params {
        backend: Arc::new(backend.clone()),
        ack_rx,
    })
    .await;
    let acked = backend.inner.acked.lock().unwrap().clone();
    assert_eq!(acked, vec![key(vm_id, 1), key(vm_id, 2)]);
}

#[tokio::test(start_paused = true)]
async fn retries_failed_ack_batches() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend
        .inner
        .ack_responses
        .lock()
        .unwrap()
        .push_back(Err(MockAckError));

    let (tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(key(vm_id, 1)).unwrap();
    drop(tx);

    super::run(super::Params {
        backend: Arc::new(backend.clone()),
        ack_rx,
    })
    .await;
    let acked = backend.inner.acked.lock().unwrap().clone();
    assert_eq!(acked, vec![key(vm_id, 1)]);
}
