use waymark_action_runtime_core::ActionCallCompletionsProvider as _;
use waymark_action_runtime_core::{ActionCallLossError, ActionCallStage};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{CompletionsProvider, ProviderGoneError, Reporter};

type TestReporter = Reporter<u64, i64, String>;

type TestProvider = CompletionsProvider<u64, i64, String>;

fn new() -> (TestReporter, TestProvider) {
    super::new()
}

fn promise_state_id_from_raw(raw: usize) -> PromiseStateId {
    index_type::IndexType::from_raw_index(raw)
}

fn correlation(vm_id: u64, promise: usize) -> WithVmId<u64, ActionCallCorrelation> {
    WithVmId {
        vm_id,
        inner: ActionCallCorrelation {
            effect_number: EffectNumber(7),
            promise_state_id: promise_state_id_from_raw(promise),
        },
    }
}

#[tokio::test]
async fn a_report_comes_out_as_a_loss_of_its_stage_under_its_correlation() {
    let (reporter, mut provider) = new();

    reporter
        .report_loss(correlation(42, 1), ActionCallStage::Unknown)
        .unwrap();
    reporter
        .report_loss(correlation(42, 2), ActionCallStage::NotStarted)
        .unwrap();

    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    let reported: Vec<_> = completions
        .into_iter()
        .map(|completion| {
            let Err(loss) = completion.execution_result else {
                panic!("a lost execution carries no outcome");
            };

            (completion.metadata, loss)
        })
        .collect();
    assert_eq!(
        reported,
        vec![
            (
                correlation(42, 1),
                ActionCallLossError {
                    stage: ActionCallStage::Unknown,
                },
            ),
            (
                correlation(42, 2),
                ActionCallLossError {
                    stage: ActionCallStage::NotStarted,
                },
            ),
        ]
    );
}

#[tokio::test]
async fn reports_already_there_come_out_in_one_wait() {
    let (reporter, mut provider) = new();
    let other_reporter = reporter.clone();

    reporter
        .report_loss(correlation(42, 1), ActionCallStage::Unknown)
        .unwrap();
    other_reporter
        .report_loss(correlation(43, 2), ActionCallStage::Unknown)
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
        vec![
            (42, promise_state_id_from_raw(1)),
            (43, promise_state_id_from_raw(2))
        ]
    );
}

#[tokio::test]
async fn the_provider_shuts_down_once_every_reporter_is_gone() {
    let (reporter, mut provider) = new();
    let other_reporter = reporter.clone();
    drop(other_reporter);

    reporter
        .report_loss(correlation(42, 1), ActionCallStage::Unknown)
        .unwrap();
    drop(reporter);

    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    assert_eq!(completions.len().get(), 1);
    assert!(provider.wait_for_completions().await.unwrap().is_none());
}

#[tokio::test]
async fn a_report_fails_once_the_provider_is_gone() {
    let (reporter, provider) = new();
    drop(provider);

    let result = reporter.report_loss(correlation(42, 1), ActionCallStage::Unknown);

    assert!(matches!(result, Err(ProviderGoneError)));
}
