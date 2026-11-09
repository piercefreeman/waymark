//! Reporting action calls as lost executions, and the completions
//! provider those reports come out of.

#[cfg(test)]
mod tests;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallLossError, ActionCallStage};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};

/// The completion a [`Reporter`] reports for a call: the call's
/// correlation, and the loss with what is known about how far it got.
pub type Completion<VmId, Value, RaisedException> = ActionCallCompletion<
    WithVmId<VmId, ActionCallCorrelation>,
    Value,
    RaisedException,
    ActionCallLossError,
>;

/// What [`new`] opens: a [`Reporter`], to clone per VM, and the
/// [`CompletionsProvider`] the reports come out of.
pub type ReporterAndProvider<VmId, Value, RaisedException> = (
    Reporter<VmId, Value, RaisedException>,
    CompletionsProvider<VmId, Value, RaisedException>,
);

/// Open the reports channel.
pub fn new<VmId, Value, RaisedException>() -> ReporterAndProvider<VmId, Value, RaisedException> {
    let (completions_tx, completions_rx) = mpsc::unbounded_channel();

    (
        Reporter { completions_tx },
        CompletionsProvider { completions_rx },
    )
}

/// The [`CompletionsProvider`] is gone: no report can be delivered.
#[derive(Debug, thiserror::Error)]
#[error("the lost-execution completions provider is gone")]
pub struct ProviderGoneError;

/// Reports action calls as lost executions.
#[derive(Clone)]
pub struct Reporter<VmId, Value, RaisedException> {
    /// The sending side of the reports channel.
    pub completions_tx: mpsc::UnboundedSender<Completion<VmId, Value, RaisedException>>,
}

impl<VmId, Value, RaisedException> Reporter<VmId, Value, RaisedException> {
    /// Report the call under `metadata` as a lost execution, with `stage`
    /// as what is known about how far it got.
    pub fn report_loss(
        &self,
        metadata: WithVmId<VmId, ActionCallCorrelation>,
        stage: ActionCallStage,
    ) -> Result<(), ProviderGoneError> {
        let completion = ActionCallCompletion {
            metadata,
            execution_result: Err(ActionCallLossError { stage }),
        };

        self.completions_tx
            .send(completion)
            .map_err(|_: mpsc::error::SendError<_>| ProviderGoneError)
    }
}

/// The completions provider the [`Reporter`]s' reports come out of.
pub struct CompletionsProvider<VmId, Value, RaisedException> {
    /// The receiving side of the reports channel.
    pub completions_rx: mpsc::UnboundedReceiver<Completion<VmId, Value, RaisedException>>,
}

impl<VmId, Value, RaisedException> waymark_action_runtime_core::ActionCallCompletionsProvider
    for CompletionsProvider<VmId, Value, RaisedException>
where
    VmId: Send,
    Value: Send,
    RaisedException: Send,
{
    type Value = Value;
    type RaisedException = RaisedException;
    type ActionExecutionError = ActionCallLossError;
    type WaitError = core::convert::Infallible;
    type Metadata = WithVmId<VmId, ActionCallCorrelation>;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<Option<NEVec<Completion<VmId, Value, RaisedException>>>, core::convert::Infallible>
    {
        let Some(first) = self.completions_rx.recv().await else {
            return Ok(None);
        };

        let mut completions = NEVec::new(first);
        while let Ok(completion) = self.completions_rx.try_recv() {
            completions.push(completion);
        }

        Ok(Some(completions))
    }
}
