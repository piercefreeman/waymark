//! Tests for the top-level EffectHandler, PromiseSettler, and Ack types.

use nonempty_collections::NEVec;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_driver_core::{EffectHandler as _, PromiseSettler as _};
use waymark_vm_interpreter_extcallset::Effect;
use waymark_vm_runtime_effect::{EffectNumber, EmittedEffect};
use waymark_vm_runtime_promise_core::PromiseStateId;

// ------------------------------------------------------------------
// EffectHandler and PromiseSettler tests
// ------------------------------------------------------------------

use mockall::mock;
use waymark_action_runtime_core::{
    ActionCallCompletion, ActionCallCompletionsProvider, ActionCallRequest, ActionCallRequester,
};

/// Error type for the mock action requester.
#[derive(Debug, thiserror::Error)]
#[error("mock requester error")]
pub struct MockRequesterError;

/// Error type for the mock completions provider.
#[derive(Debug, thiserror::Error)]
#[error("mock provider error")]
pub struct MockProviderError;

mock! {
    pub ActionRequester {}

    impl ActionCallRequester for ActionRequester {
        type Error = MockRequesterError;
        type Argument = waymark_vm_value::ReadyValue;

        async fn request_action_call(
            &self,
            request: ActionCallRequest<waymark_vm_value::ReadyValue>,
        ) -> Result<(), MockRequesterError>;
    }
}

mock! {
    pub CompletionsProvider {}

    impl ActionCallCompletionsProvider for CompletionsProvider {
        type Value = waymark_vm_value::ReadyValue;
        type Error = MockProviderError;

        async fn wait_for_completions(
            &mut self,
        ) -> Result<
            NEVec<ActionCallCompletion<waymark_vm_value::ReadyValue>>,
            MockProviderError,
        >;
    }
}

#[tokio::test]
async fn effect_handler_dispatches_action_call() {
    let mut requester = MockActionRequester::new();
    requester.expect_request_action_call().returning(|_| Ok(()));

    let provider = MockCompletionsProvider::new();

    let (action_handler, action_poller) = waymark_action_reconciler::new(requester, provider);
    let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
    let (mut handler, _settler) =
        crate::new(action_handler, sleep_handler, action_poller, sleep_poller);

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
    let requester = MockActionRequester::new();

    let mut provider = MockCompletionsProvider::new();
    provider
        .expect_wait_for_completions()
        .returning(|| Err(MockProviderError));

    let (action_handler, action_poller) = waymark_action_reconciler::new(requester, provider);
    let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
    let (mut handler, mut settler) =
        crate::new(action_handler, sleep_handler, action_poller, sleep_poller);

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
    let requester = MockActionRequester::new();

    let mut provider = MockCompletionsProvider::new();
    provider
        .expect_wait_for_completions()
        .returning(|| Err(MockProviderError));

    let (action_handler, action_poller) = waymark_action_reconciler::new(requester, provider);
    let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
    let (handler, mut settler) =
        crate::new(action_handler, sleep_handler, action_poller, sleep_poller);
    drop(handler);

    let result = settler
        .get_promise_settlements(NEVec::new(PromiseStateId(0)))
        .await;

    assert!(
        matches!(result, Err(crate::NoSettlementsError::NoPendingPromises)),
        "expected NoPendingPromises"
    );
}
