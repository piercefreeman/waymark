use nonempty_collections::{NEVec, nev};
use tokio::sync::mpsc;
use waymark_action_runtime_core::{
    ActionCallCompletion, ActionCallCompletionsProvider, ActionCallOutcome,
};

use super::{AggregateActionCallCompletionsProvider, WaitError};

type TestCompletion = ActionCallCompletion<u8, i64, String, core::convert::Infallible>;

/// A side fed through a channel: a batch per message, `None` once the
/// sender is gone, and an error on request.
struct ChannelSide {
    batches_rx: mpsc::UnboundedReceiver<Result<NEVec<TestCompletion>, SideError>>,
}

#[derive(Debug, thiserror::Error)]
#[error("side failure {0}")]
struct SideError(u8);

impl ActionCallCompletionsProvider for ChannelSide {
    type Value = i64;
    type RaisedException = String;
    type ActionExecutionError = core::convert::Infallible;
    type WaitError = SideError;
    type Metadata = u8;

    async fn wait_for_completions(&mut self) -> Result<Option<NEVec<TestCompletion>>, SideError> {
        match self.batches_rx.recv().await {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => Err(error),
        }
    }
}

type BatchTx = mpsc::UnboundedSender<Result<NEVec<TestCompletion>, SideError>>;

fn aggregate() -> (
    AggregateActionCallCompletionsProvider<ChannelSide, ChannelSide>,
    BatchTx,
    BatchTx,
) {
    let (first_tx, first_rx) = mpsc::unbounded_channel();
    let (second_tx, second_rx) = mpsc::unbounded_channel();
    let provider = AggregateActionCallCompletionsProvider::new(
        ChannelSide {
            batches_rx: first_rx,
        },
        ChannelSide {
            batches_rx: second_rx,
        },
    );

    (provider, first_tx, second_tx)
}

fn batch(metadata: u8) -> NEVec<TestCompletion> {
    nev![ActionCallCompletion {
        metadata,
        execution_result: Ok(ActionCallOutcome::Value(1)),
    }]
}

fn metadata_of(completions: NEVec<TestCompletion>) -> Vec<u8> {
    completions
        .into_iter()
        .map(|completion| completion.metadata)
        .collect()
}

#[tokio::test]
async fn a_batch_comes_from_whichever_side_has_one() {
    let (mut provider, first_tx, second_tx) = aggregate();

    second_tx.send(Ok(batch(2))).unwrap();
    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    assert_eq!(metadata_of(completions), vec![2]);

    first_tx.send(Ok(batch(1))).unwrap();
    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    assert_eq!(metadata_of(completions), vec![1]);
}

#[tokio::test]
async fn a_side_that_shut_down_leaves_the_other_served() {
    let (mut provider, first_tx, second_tx) = aggregate();
    drop(first_tx);

    second_tx.send(Ok(batch(2))).unwrap();
    let completions = provider.wait_for_completions().await.unwrap().unwrap();
    assert_eq!(metadata_of(completions), vec![2]);
    assert!(provider.first.is_none());
    assert!(provider.second.is_some());
}

#[tokio::test]
async fn both_sides_shut_down_shuts_the_aggregate_down() {
    let (mut provider, first_tx, second_tx) = aggregate();
    drop(first_tx);
    drop(second_tx);

    assert!(provider.wait_for_completions().await.unwrap().is_none());
    assert!(provider.first.is_none());
    assert!(provider.second.is_none());
}

#[tokio::test]
async fn a_failed_wait_reports_its_side() {
    let (mut provider, _first_tx, second_tx) = aggregate();

    second_tx.send(Err(SideError(7))).unwrap();
    let Err(error) = provider.wait_for_completions().await else {
        panic!("the failed side's error is reported");
    };

    assert!(matches!(error, WaitError::Second(SideError(7))));
}
