use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use nonempty_collections::NEVec;
use waymark_extcall_reconciler_core::SleepPromiseSettler;
use waymark_ids::InstanceId;
use waymark_sleep_compat_python::ReadyValueSleepProvider;
use waymark_sleep_reconciler_backend::SleepKey;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement, PromiseSettlementAck as _};
use waymark_vm_runtime_promise_core::PromiseStateId;
use waymark_vm_value_python::ReadyValue;

use super::{Ack, DemandRegistrar, Params, PollSleepSettlementsError, SettlementsHandle};
use crate::test_support::{MockBackend, key};

fn demand(ids: &[usize]) -> NEVec<PromiseStateId> {
    NEVec::try_from_vec(ids.iter().map(|id| PromiseStateId(*id)).collect())
        .expect("test demand is non-empty")
}

/// Poll a future exactly once with a no-op waker.
fn poll_once<F: Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Poll the handle for settlements with the given demanded promise ids.
async fn poll_settlements(
    handle: &mut SettlementsHandle<InstanceId, ReadyValueSleepProvider>,
    ids: &[usize],
) -> Result<NEVec<PromiseSettlement<ReadyValue, Ack<InstanceId>>>, PollSleepSettlementsError> {
    let demand = demand(ids);
    SleepPromiseSettler::<Ack<InstanceId>>::poll_sleep_settlements(
        handle,
        demand.as_nonempty_slice(),
    )
    .await
}

fn poller(
    backend: &MockBackend,
) -> (
    DemandRegistrar<InstanceId>,
    Params<MockBackend>,
    tokio::sync::mpsc::UnboundedReceiver<SleepKey<InstanceId>>,
) {
    let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let (registrar, state) = super::state(ack_tx);
    let params = Params {
        backend: Arc::new(backend.clone()),
        state,
        poll_interval: waymark_nonzero_duration::NonZeroDuration::try_from(
            std::time::Duration::from_millis(250),
        )
        .unwrap(),
    };
    (registrar, params, ack_rx)
}

#[tokio::test]
async fn delivers_demanded_settlements_with_key_acks() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend
        .inner
        .poll_batches
        .lock()
        .unwrap()
        .push_back(vec![key(vm_id, 3)]);

    let (registrar, params, mut ack_rx) = poller(&backend);
    let mut handle = registrar.subscribe::<ReadyValueSleepProvider>(vm_id);

    let poll_loop = tokio::spawn(super::run(params));

    let settlements = poll_settlements(&mut handle, &[3])
        .await
        .expect("settlement delivered");
    assert_eq!(settlements.len().get(), 1);
    let settlement = settlements.into_iter().next().unwrap();
    assert_eq!(settlement.promise_state_id, PromiseStateId(3));
    assert!(matches!(
        settlement.resolution,
        PromiseResolution::Resolved(ReadyValue::None)
    ));

    // The ack originates from the poller: acknowledging pushes the row's
    // own key onto the poller's ack channel.
    assert!(ack_rx.try_recv().is_err());
    settlement.ack.acknowledge_promise_settlement();
    assert_eq!(ack_rx.try_recv().unwrap(), key(vm_id, 3));

    poll_loop.abort();
}

#[tokio::test]
async fn parks_without_subscribed_demand() {
    let backend = MockBackend::default();
    let (registrar, params, _ack_rx) = poller(&backend);
    // A subscribed VM with no demand is not demand.
    let _handle = registrar.subscribe::<ReadyValueSleepProvider>(InstanceId::new_uuid_v4());

    {
        let mut run = pin!(super::run(params));
        for _ in 0..8 {
            assert!(poll_once(run.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    }

    assert_eq!(backend.inner.poll_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn buffered_keys_survive_a_cancelled_wait() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend
        .inner
        .poll_batches
        .lock()
        .unwrap()
        .push_back(vec![key(vm_id, 3)]);

    let (registrar, params, _ack_rx) = poller(&backend);
    let mut handle = registrar.subscribe::<ReadyValueSleepProvider>(vm_id);

    {
        // Register demand, then cancel the wait before delivery.
        let mut wait = pin!(poll_settlements(&mut handle, &[3]));
        assert!(poll_once(wait.as_mut()).is_pending());
    }

    // Drive the poller until it has fetched and buffered the key.
    {
        let mut run = pin!(super::run(params));
        for _ in 0..8 {
            assert!(poll_once(run.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    }

    // The next call settles from the buffer without further polling.
    let settlements = poll_settlements(&mut handle, &[3])
        .await
        .expect("buffered settlement returned");
    assert_eq!(settlements.len().get(), 1);
}

#[tokio::test]
async fn stale_handle_drop_leaves_resubscribed_entry_intact() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    backend
        .inner
        .poll_batches
        .lock()
        .unwrap()
        .push_back(vec![key(vm_id, 3)]);

    let (registrar, params, _ack_rx) = poller(&backend);

    // The VM is re-subscribed while the previous handle is still
    // alive; dropping the stale handle must not unsubscribe the fresh
    // one.
    let stale_handle = registrar.subscribe::<ReadyValueSleepProvider>(vm_id);
    let mut fresh_handle = registrar.subscribe::<ReadyValueSleepProvider>(vm_id);
    drop(stale_handle);

    let poll_loop = tokio::spawn(super::run(params));

    let settlements = poll_settlements(&mut fresh_handle, &[3])
        .await
        .expect("fresh handle still receives settlements");
    assert_eq!(settlements.len().get(), 1);

    poll_loop.abort();
}

#[tokio::test]
async fn waiters_fail_when_the_run_loop_is_cancelled() {
    let vm_id = InstanceId::new_uuid_v4();
    let backend = MockBackend::default();
    let (registrar, params, _ack_rx) = poller(&backend);
    let mut handle = registrar.subscribe::<ReadyValueSleepProvider>(vm_id);

    // Start the loop, then cancel it — the registry is marked closed.
    {
        let mut run = pin!(super::run(params));
        assert!(poll_once(run.as_mut()).is_pending());
    }

    let result = poll_settlements(&mut handle, &[1]).await;
    assert!(matches!(result, Err(PollSleepSettlementsError::PollerGone)));
}
