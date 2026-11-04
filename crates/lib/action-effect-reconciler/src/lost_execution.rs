//! A requester reporting each call handed to it as a lost execution, and
//! the completions provider those reports come out of.

#[cfg(test)]
mod tests;

use std::marker::PhantomData;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::{
    ActionCallCompletion, ActionCallLossError, ActionCallRequest, ActionCallStage,
};
use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};

/// The completion a [`Requester`] reports for a call: the call's
/// correlation, and a loss of unknown stage.
pub type Completion<VmId, Value, RaisedException> = ActionCallCompletion<
    WithVmId<VmId, ActionCallCorrelation>,
    Value,
    RaisedException,
    ActionCallLossError,
>;

/// The sending side of the reports channel.
pub type CompletionsTx<VmId, Value, RaisedException> =
    mpsc::UnboundedSender<Completion<VmId, Value, RaisedException>>;

/// What [`new`] opens: a [`Requester`], to clone per VM, and the
/// [`CompletionsProvider`] the reports come out of.
pub type RequesterAndProvider<Argument, VmId, Value, RaisedException> = (
    Requester<Argument, VmId, Value, RaisedException>,
    CompletionsProvider<VmId, Value, RaisedException>,
);

/// Open the reports channel.
///
/// `Argument` is whatever the caller decodes a call's arguments into; the
/// requester never reads them.
pub fn new<Argument, VmId, Value, RaisedException>()
-> RequesterAndProvider<Argument, VmId, Value, RaisedException> {
    let (completions_tx, completions_rx) = mpsc::unbounded_channel();

    let requester = Requester {
        completions_tx,
        _argument: PhantomData,
    };

    (requester, CompletionsProvider { completions_rx })
}

/// [`new`] with the calls' arguments skipped: the requester never reads
/// them, so the caller's decode need not build them.
pub fn new_with_ignored_payload<VmId, Value, RaisedException>()
-> RequesterAndProvider<serde::de::IgnoredAny, VmId, Value, RaisedException> {
    new()
}

/// The [`CompletionsProvider`] is gone: no report can be delivered.
#[derive(Debug, thiserror::Error)]
#[error("the lost-execution completions provider is gone")]
pub struct ProviderGoneError;

/// A requester reporting each call handed to it as a lost execution.
#[derive(Clone)]
pub struct Requester<Argument, VmId, Value, RaisedException> {
    /// The sending side of the reports channel.
    pub completions_tx: CompletionsTx<VmId, Value, RaisedException>,

    /// The argument type this requester accepts.
    pub _argument: PhantomData<Argument>,
}

impl<Argument, VmId, Value, RaisedException> waymark_action_runtime_core::ActionCallRequester
    for Requester<Argument, VmId, Value, RaisedException>
where
    Argument: Send + Sync,
    VmId: Send,
    Value: Send,
    RaisedException: Send,
{
    type Error = ProviderGoneError;
    type Argument = Argument;
    type Metadata = WithVmId<VmId, ActionCallCorrelation>;

    async fn request_action_call(
        &self,
        request: ActionCallRequest<Argument, Self::Metadata>,
    ) -> Result<(), ProviderGoneError> {
        let ActionCallRequest {
            action_ref: _,
            arguments: _,
            metadata,
        } = request;

        let completion = ActionCallCompletion {
            metadata,
            execution_result: Err(ActionCallLossError {
                stage: ActionCallStage::Unknown,
            }),
        };

        self.completions_tx
            .send(completion)
            .map_err(|_: mpsc::error::SendError<_>| ProviderGoneError)
    }
}

/// The completions provider the [`Requester`]s' reports come out of.
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
