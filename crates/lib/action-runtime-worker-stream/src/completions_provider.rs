use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};
use waymark_action_runtime_metadata_codec::Decode;
use waymark_convert_core::Convert as _;
use waymark_proto::messages as proto;

/// Error returned when receiving action results fails.
#[derive(Debug, thiserror::Error)]
pub enum ReceiveError<DecodeError> {
    /// The result channel was closed.
    #[error("action result channel closed")]
    ChannelClosed,

    /// A result carried correlation metadata that could not be decoded, so it
    /// cannot be routed back to the promise that awaits it.
    #[error("unable to decode correlation metadata for an action completion")]
    Decode(DecodeError),
}

/// Receives action results from a tokio mpsc channel and surfaces them
/// as [`waymark_action_runtime_core::ActionCallCompletion`]s.
pub struct WorkerStreamActionCallCompletionsProvider<Metadata> {
    /// The receiver of the action results.
    pub rx: mpsc::Receiver<proto::ActionResult>,

    /// Phantom data for the metadata type parameter.
    _metadata: core::marker::PhantomData<Metadata>,
}

impl<Metadata> WorkerStreamActionCallCompletionsProvider<Metadata> {
    /// Create a new completions provider from a receiver.
    pub fn new(rx: mpsc::Receiver<proto::ActionResult>) -> Self {
        Self {
            rx,
            _metadata: core::marker::PhantomData,
        }
    }
}

impl<Metadata> waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerStreamActionCallCompletionsProvider<Metadata>
where
    Metadata: Decode + Send + 'static,
    <Metadata as Decode>::Error: Send + 'static,
{
    type Value = waymark_vm_value_python::ReadyValue;
    type Error = ReceiveError<<Metadata as Decode>::Error>;
    type Metadata = Metadata;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        // Block until at least one result arrives, then drain any others that
        // are immediately available.  A decode failure on any of them is fatal
        // — the correlation is the only route back to the awaiting promise, so
        // we surface it rather than strand the promise by dropping the result.
        let result = self.rx.recv().await.ok_or(ReceiveError::ChannelClosed)?;

        let mut batch = NEVec::new(completion_from_result::<Metadata>(&result)?);

        while let Ok(result) = self.rx.try_recv() {
            batch.push(completion_from_result::<Metadata>(&result)?);
        }

        Ok(batch)
    }
}

fn completion_from_result<Metadata: Decode>(
    result: &proto::ActionResult,
) -> Result<
    ActionCallCompletion<waymark_vm_value_python::ReadyValue, Metadata>,
    ReceiveError<Metadata::Error>,
> {
    let mut bytes: &[u8] = &result.metadata;
    let metadata = Metadata::decode(&mut bytes).map_err(|error| {
        tracing::error!(
            ?result,
            %error,
            "unable to decode correlation metadata for an action completion"
        );
        ReceiveError::Decode(error)
    })?;
    let outcome = waymark_action_runtime_convert::Converter::convert(result);
    Ok(ActionCallCompletion { metadata, outcome })
}

#[cfg(test)]
mod tests {
    use waymark_action_runtime_core::ActionCallCompletionsProvider as _;
    use waymark_action_runtime_metadata::ActionCallCorrelation;

    use super::*;

    #[tokio::test]
    async fn undecodable_metadata_surfaces_as_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut provider =
            WorkerStreamActionCallCompletionsProvider::<ActionCallCorrelation>::new(rx);

        // A result whose metadata is not the fixed-length correlation encoding
        // (here absent, i.e. empty) cannot be routed to its promise, so it must
        // surface as an error rather than be dropped (which would strand the
        // awaiting promise forever).
        tx.send(proto::ActionResult {
            metadata: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();

        let result = provider.wait_for_completions().await;
        assert!(
            matches!(result, Err(ReceiveError::Decode(_))),
            "expected a decode error"
        );
    }

    #[tokio::test]
    async fn closed_channel_surfaces_as_error() {
        let (tx, rx) = mpsc::channel::<proto::ActionResult>(1);
        let mut provider =
            WorkerStreamActionCallCompletionsProvider::<ActionCallCorrelation>::new(rx);
        drop(tx);

        let result = provider.wait_for_completions().await;
        assert!(matches!(result, Err(ReceiveError::ChannelClosed)));
    }
}
