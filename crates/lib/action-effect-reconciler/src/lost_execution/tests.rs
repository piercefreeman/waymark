use waymark_action_runtime_core::ActionCallCompletionsProvider as _;
use waymark_action_runtime_core::ActionCallRequester as _;
use waymark_action_runtime_core::{ActionCallLossError, ActionCallRequest, ActionCallStage};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{CompletionsProvider, ProviderGoneError, Requester};
use crate::test_support::action_ref;

type TestRequester = Requester<i64, u64, i64, String>;

type TestProvider = CompletionsProvider<u64, i64, String>;

fn new() -> (TestRequester, TestProvider) {
    super::new()
}

fn correlation(vm_id: u64, promise: usize) -> WithVmId<u64, ActionCallCorrelation> {
    WithVmId {
        vm_id,
        inner: ActionCallCorrelation {
            effect_number: EffectNumber(7),
            promise_state_id: PromiseStateId(promise),
        },
    }
}

fn request(
    vm_id: u64,
    promise: usize,
) -> ActionCallRequest<i64, WithVmId<u64, ActionCallCorrelation>> {
    ActionCallRequest {
        action_ref: action_ref("act"),
        arguments: vec![1, 2],
        metadata: correlation(vm_id, promise),
    }
}

#[tokio::test]
async fn a_call_comes_out_as_a_loss_of_unknown_stage_under_its_correlation() {
    let (requester, mut provider) = new();

    requester.request_action_call(request(42, 1)).await.unwrap();

    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    assert_eq!(completions.len().get(), 1);
    let completion = completions.into_iter().next().unwrap();
    assert_eq!(completion.metadata, correlation(42, 1));
    let Err(loss) = completion.execution_result else {
        panic!("a lost execution carries no outcome");
    };
    assert_eq!(
        loss,
        ActionCallLossError {
            stage: ActionCallStage::Unknown,
        },
    );
}

#[tokio::test]
async fn reports_already_there_come_out_in_one_wait() {
    let (requester, mut provider) = new();
    let other_requester = requester.clone();

    requester.request_action_call(request(42, 1)).await.unwrap();
    other_requester
        .request_action_call(request(43, 2))
        .await
        .unwrap();

    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    let promises: Vec<_> = completions
        .into_iter()
        .map(|completion| {
            (
                completion.metadata.vm_id,
                completion.metadata.inner.promise_state_id,
            )
        })
        .collect();
    assert_eq!(
        promises,
        vec![(42, PromiseStateId(1)), (43, PromiseStateId(2))]
    );
}

#[tokio::test]
async fn the_provider_shuts_down_once_every_requester_is_gone() {
    let (requester, mut provider) = new();
    let other_requester = requester.clone();
    drop(other_requester);

    requester.request_action_call(request(42, 1)).await.unwrap();
    drop(requester);

    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    assert_eq!(completions.len().get(), 1);
    assert!(provider.wait_for_completions().await.unwrap().is_none());
}

#[tokio::test]
async fn a_report_fails_once_the_provider_is_gone() {
    let (requester, provider) = new();
    drop(provider);

    let result = requester.request_action_call(request(42, 1)).await;

    assert!(matches!(result, Err(ProviderGoneError)));
}
