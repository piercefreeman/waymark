//! Tests for the top-level EffectHandler, PromiseSettler, and Ack types.

use std::sync::Arc;

use waymark_extcall_core::ActionRef;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_driver_core::{EffectHandler as _, PromiseSettlementAck, PromiseSettler as _};
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::{Ack, EffectHandler as Handler};

// ------------------------------------------------------------------
// Ack tests
// ------------------------------------------------------------------

#[tokio::test]
async fn ack_action_sends_on_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let token = uuid::Uuid::new_v4();

    let ack = Ack::Action(token, tx);
    ack.acknowledge_promise_settlement();

    let received = rx.try_recv().unwrap();
    assert_eq!(received, token);
}

#[tokio::test]
async fn ack_sleep_is_noop() {
    let ack = Ack::Sleep;
    // Should not panic — sleep ack is a no-op.
    ack.acknowledge_promise_settlement();
}

// ------------------------------------------------------------------
// EffectHandler tests (require a mock worker pool)
// ------------------------------------------------------------------

use mockall::mock;
use nonempty_collections::NEVec;
use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

mock! {
    WorkerPool {}

    impl BaseWorkerPool for WorkerPool {
        fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

        async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>>;
    }
}

#[tokio::test]
async fn effect_handler_dispatches_action_call() {
    let mut mock = MockWorkerPool::new();
    mock.expect_queue().returning(|_| Ok(()));

    let (action_handler, _action_poller) = crate::action_call::new(mock);
    let (sleep_handler, _sleep_poller) = crate::sleep::new();

    let mut handler = Handler::<
        MockWorkerPool,
        waymark_extcall_convert::Converter,
        serde_json::Value,
    >::new(Arc::new(action_handler), Arc::new(sleep_handler));

    let effect = Effect::ActionCall {
        promise_state_id: PromiseStateId(0),
        action_ref: ActionRef {
            action_name: "test".into(),
            module_name: None,
            call_args: vec!["arg".into()],
            timeout_seconds: 30,
            max_retries: 0,
            exception_types: vec![],
        },
        args: vec![serde_json::Value::String("hi".into())],
    };

    handler.handle_effect(effect).await.unwrap();
}

#[tokio::test]
async fn effect_handler_records_sleep() {
    let mock = {
        let mut m = MockWorkerPool::new();
        m.expect_queue().returning(|_| Ok(()));
        m
    };

    let (action_handler, _action_poller) = crate::action_call::new(mock);
    let (sleep_handler, mut sleep_poller) = crate::sleep::new();

    let mut handler = Handler::<
        MockWorkerPool,
        waymark_extcall_convert::Converter,
        serde_json::Value,
    >::new(Arc::new(action_handler), Arc::new(sleep_handler));

    let effect = Effect::Sleep {
        promise_state_id: PromiseStateId(0),
        duration: NonZeroDuration::try_from(std::time::Duration::from_nanos(1)).unwrap(),
    };

    handler.handle_effect(effect).await.unwrap();

    // Sleep should have been recorded — poll should return it.
    let settlements = sleep_poller
        .poll::<waymark_extcall_convert::Converter, waymark_vm_value::Value>()
        .await
        .unwrap();
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, PromiseStateId(0));
}

#[tokio::test]
async fn promise_settler_no_sources_returns_error() {
    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete().returning(|| None);

    let (action_handler, action_poller) = crate::action_call::new(mock);
    drop(action_handler); // close dispatch_rx
    let (sleep_handler, sleep_poller) = crate::sleep::new();
    drop(sleep_handler); // close sleep rx

    let mut settler = crate::PromiseSettler::<
        MockWorkerPool,
        waymark_extcall_convert::Converter,
        waymark_vm_value::Value,
    >::new(action_poller, sleep_poller);

    let result = settler
        .get_promise_settlements(nonempty_collections::NEVec::new(PromiseStateId(0)))
        .await;

    assert!(
        matches!(result, Err(crate::NoSettlementsError::NoPendingPromises)),
        "expected NoPendingPromises"
    );
}
