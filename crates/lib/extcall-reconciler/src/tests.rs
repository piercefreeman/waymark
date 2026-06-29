//! Tests for the top-level EffectHandler, PromiseSettler, and Ack types.

use nonempty_collections::NEVec;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_driver_core::{EffectHandler as _, PromiseSettlementAck, PromiseSettler as _};
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_effect::{EffectNumber, EmittedEffect};
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::Ack;

// ------------------------------------------------------------------
// Ack tests
// ------------------------------------------------------------------

#[tokio::test]
async fn ack_action_is_noop() {
    let ack = Ack::Action;
    ack.acknowledge_promise_settlement();
}

#[tokio::test]
async fn ack_sleep_is_noop() {
    let ack = Ack::Sleep;
    ack.acknowledge_promise_settlement();
}

// ------------------------------------------------------------------
// EffectHandler tests (require a mock worker pool)
// ------------------------------------------------------------------

use mockall::mock;
use waymark_worker_core::{ActionCompletion, ActionRequest, BaseWorkerPool, WorkerPoolError};

mock! {
    pub WorkerPool {}

    impl BaseWorkerPool for WorkerPool {
        fn queue(&self, request: ActionRequest) -> Result<(), WorkerPoolError>;

        async fn poll_complete(&self) -> Option<NEVec<ActionCompletion>>;
    }
}

fn make_requester(
    mock: MockWorkerPool,
) -> waymark_action_runtime_worker_pool::WorkerPoolRequester<std::sync::Arc<MockWorkerPool>> {
    waymark_action_runtime_worker_pool::WorkerPoolRequester {
        pool: std::sync::Arc::new(mock),
        executor_id: waymark_ids::InstanceId::new_uuid_v4(),
    }
}

fn make_provider(
    mock: MockWorkerPool,
) -> waymark_action_runtime_worker_pool::WorkerPoolCompletionsProvider<std::sync::Arc<MockWorkerPool>>
{
    waymark_action_runtime_worker_pool::WorkerPoolCompletionsProvider {
        pool: std::sync::Arc::new(mock),
    }
}

#[tokio::test]
async fn effect_handler_dispatches_action_call() {
    let mut mock = MockWorkerPool::new();
    mock.expect_queue().returning(|_| Ok(()));

    let requester = make_requester(mock);
    let mock = MockWorkerPool::new();
    let provider = make_provider(mock);

    let (mut handler, _settler) = crate::new((), requester, provider);

    let effect = Effect::ActionCall {
        promise_state_id: PromiseStateId(0),
        action_ref: waymark_action_core::ActionRef {
            action_name: "test".into(),
            module_name: None,
            call_args: vec!["arg".into()],
            timeout_seconds: 30,
            max_retries: 0,
            exception_types: vec![],
        },
        args: vec![waymark_vm_value::ReadyValue::String("hi".into())],
    };

    let effect_number = EffectNumber(0);

    handler
        .handle_effect(EmittedEffect {
            effect,
            number: effect_number,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn effect_handler_records_sleep() {
    let requester_mock = {
        let mut m = MockWorkerPool::new();
        m.expect_queue().returning(|_| Ok(()));
        m
    };
    let requester = make_requester(requester_mock);

    let provider_mock = {
        let mut m = MockWorkerPool::new();
        m.expect_poll_complete().returning(|| None);
        m
    };
    let provider = make_provider(provider_mock);

    let (mut handler, mut settler) = crate::new((), requester, provider);

    let effect = Effect::Sleep {
        promise_state_id: PromiseStateId(0),
        duration: NonZeroDuration::try_from(std::time::Duration::from_nanos(1)).unwrap(),
    };

    let effect_number = EffectNumber(0);

    handler
        .handle_effect(EmittedEffect {
            effect,
            number: effect_number,
        })
        .await
        .unwrap();

    let settlements = settler
        .get_promise_settlements(NEVec::new(PromiseStateId(0)))
        .await
        .unwrap();
    assert_eq!(settlements.len().get(), 1);
    assert_eq!(settlements[0].promise_state_id, PromiseStateId(0));
}

#[tokio::test]
async fn promise_settler_no_sources_returns_error() {
    let mock = {
        let mut m = MockWorkerPool::new();
        m.expect_queue().returning(|_| Ok(()));
        m
    };
    let requester = make_requester(mock);
    let mut mock = MockWorkerPool::new();
    mock.expect_poll_complete().returning(|| None);
    let provider = make_provider(mock);

    let (handler, mut settler) = crate::new((), requester, provider);
    drop(handler);

    let result = settler
        .get_promise_settlements(NEVec::new(PromiseStateId(0)))
        .await;

    assert!(
        matches!(result, Err(crate::NoSettlementsError::NoPendingPromises)),
        "expected NoPendingPromises"
    );
}
