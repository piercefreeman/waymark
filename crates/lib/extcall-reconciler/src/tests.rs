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
use waymark_action_runtime_metadata::ActionCallCorrelation;

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
        type Metadata = ActionCallCorrelation;

        async fn request_action_call(
            &self,
            request: ActionCallRequest<waymark_vm_value::ReadyValue, ActionCallCorrelation>,
        ) -> Result<(), MockRequesterError>;
    }
}

/// Hand-rolled completions-provider fake with precisely scripted behaviors.
enum FakeCompletionsProvider {
    /// Never yields a completion.
    Pending,

    /// Immediately fails with [`MockProviderError`].
    Failing,
}

impl ActionCallCompletionsProvider for FakeCompletionsProvider {
    type Value = waymark_vm_value::ReadyValue;
    type Error = MockProviderError;
    type Metadata = ActionCallCorrelation;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<
        NEVec<ActionCallCompletion<waymark_vm_value::ReadyValue, ActionCallCorrelation>>,
        MockProviderError,
    > {
        match self {
            Self::Pending => {
                std::future::pending::<()>().await;
                unreachable!("pending never resolves")
            }
            Self::Failing => Err(MockProviderError),
        }
    }
}

#[tokio::test]
async fn effect_handler_dispatches_action_call() {
    let mut requester = MockActionRequester::new();
    requester.expect_request_action_call().returning(|_| Ok(()));

    let provider = FakeCompletionsProvider::Pending;

    let action_handler = waymark_extcall_reconciler_action_compat::EffectHandler::new(requester);
    let action_poller = waymark_extcall_reconciler_action_compat::PromiseSettler::new(provider);
    let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
    let (mut handler, _settler) =
        crate::new(action_handler, sleep_handler, action_poller, sleep_poller);

    let effect = Effect::ActionCall {
        promise_state_id: PromiseStateId(0),
        action_ref: waymark_action_core::ActionRef {
            runtime: waymark_action_core::ActionRuntime::Python,
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

    // The action poller must not complete during this test.
    let provider = FakeCompletionsProvider::Pending;

    let action_handler = waymark_extcall_reconciler_action_compat::EffectHandler::new(requester);
    let action_poller = waymark_extcall_reconciler_action_compat::PromiseSettler::new(provider);
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
async fn action_settler_error_propagates() {
    let requester = MockActionRequester::new();

    let provider = FakeCompletionsProvider::Failing;

    let action_handler = waymark_extcall_reconciler_action_compat::EffectHandler::new(requester);
    let action_poller = waymark_extcall_reconciler_action_compat::PromiseSettler::new(provider);
    let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
    let (_handler, mut settler) =
        crate::new(action_handler, sleep_handler, action_poller, sleep_poller);
    // Handler kept alive — sleep poller blocks waiting for sleeps.
    // Action provider errors immediately — its error wins tokio::select!.

    let result = settler
        .get_promise_settlements(NEVec::new(PromiseStateId(0)))
        .await;

    assert!(
        matches!(result, Err(crate::GetPromiseSettlementsError::Action(_))),
        "expected Action error from GetPromiseSettlementsError"
    );
}

#[tokio::test]
async fn sleep_settler_error_propagates() {
    let requester = MockActionRequester::new();

    // The action poller must not complete during this test.
    let provider = FakeCompletionsProvider::Pending;

    let action_handler = waymark_extcall_reconciler_action_compat::EffectHandler::new(requester);
    let action_poller = waymark_extcall_reconciler_action_compat::PromiseSettler::new(provider);
    let (sleep_handler, sleep_poller) = waymark_sleep_reconciler::new(false);
    let (handler, mut settler) =
        crate::new(action_handler, sleep_handler, action_poller, sleep_poller);
    drop(handler); // Close sleep channel — sleep poller errors with ChannelClosed.
    // Action provider never completes — sleep error wins tokio::select!.

    let result = settler
        .get_promise_settlements(NEVec::new(PromiseStateId(0)))
        .await;

    assert!(
        matches!(result, Err(crate::GetPromiseSettlementsError::Sleep(_))),
        "expected Sleep error from GetPromiseSettlementsError"
    );
}
