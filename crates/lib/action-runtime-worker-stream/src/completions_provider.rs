use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};
use waymark_action_runtime_metadata_codec::Decode;
use waymark_convert_core::TryConvert;
use waymark_proto::messages as proto;

/// The error of the payload conversion the provider delegates to.
///
/// Expressed as a projection through the value converter rather than
/// named concretely: this provider merely propagates that conversion's
/// failure, whatever it is.
pub type PayloadConvertErrorFor<ValueConverter, Value> = waymark_convert_core::ConvertErrorFor<
    ValueConverter,
    Vec<u8>,
    waymark_action_runtime_core::ActionCallOutcome<Value>,
>;

/// The [`ReceiveError`] of a provider over the given metadata and value
/// converter.
pub type ReceiveErrorFor<Metadata, Value, ValueConverter> =
    ReceiveError<<Metadata as Decode>::Error, PayloadConvertErrorFor<ValueConverter, Value>>;

/// Error returned when receiving action results fails.
#[derive(Debug, thiserror::Error)]
pub enum ReceiveError<DecodeError, PayloadError> {
    /// The result channel was closed.
    #[error("action result channel closed")]
    ChannelClosed,

    /// A result carried correlation metadata that could not be decoded, so it
    /// cannot be routed back to the promise that awaits it.
    #[error("unable to decode correlation metadata for an action completion")]
    Decode(DecodeError),

    /// A result carried a payload that could not be converted, so there
    /// is nothing valid to settle the promise with.
    #[error("unable to convert an action-completion payload")]
    Payload(#[source] PayloadError),
}

/// Receives action results from a tokio mpsc channel and surfaces them
/// as [`waymark_action_runtime_core::ActionCallCompletion`]s.
pub struct WorkerStreamActionCallCompletionsProvider<Metadata, Value, ValueConverter> {
    /// The receiver of the action results.
    pub rx: mpsc::Receiver<proto::ActionResult>,

    /// Phantom data for the type parameters the provider only relays.
    _phantom: core::marker::PhantomData<(Metadata, Value, ValueConverter)>,
}

impl<Metadata, Value, ValueConverter>
    WorkerStreamActionCallCompletionsProvider<Metadata, Value, ValueConverter>
{
    /// Create a new completions provider from a receiver.
    pub fn new(rx: mpsc::Receiver<proto::ActionResult>) -> Self {
        Self {
            rx,
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<Metadata, Value, ValueConverter> waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerStreamActionCallCompletionsProvider<Metadata, Value, ValueConverter>
where
    Metadata: Decode + Send + 'static,
    <Metadata as Decode>::Error: Send + 'static,
    Value: Send,
    ValueConverter:
        TryConvert<Vec<u8>, waymark_action_runtime_core::ActionCallOutcome<Value>> + Send,
    PayloadConvertErrorFor<ValueConverter, Value>: core::fmt::Debug + Send + 'static,
{
    type Value = Value;
    // Every received result carries an outcome by construction; there is
    // no protocol report of a lost execution on the stream path.
    type ActionExecutionError = core::convert::Infallible;
    type WaitError = ReceiveErrorFor<Metadata, Value, ValueConverter>;
    type Metadata = Metadata;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::WaitError> {
        // Block until at least one result arrives, then drain any others that
        // are immediately available.  A decode failure on any of them is fatal
        // — the correlation is the only route back to the awaiting promise, so
        // we surface it rather than strand the promise by dropping the result.
        let result = self.rx.recv().await.ok_or(ReceiveError::ChannelClosed)?;

        let mut batch = NEVec::new(completion_from_result::<Metadata, Value, ValueConverter>(
            result,
        )?);

        while let Ok(result) = self.rx.try_recv() {
            batch.push(completion_from_result::<Metadata, Value, ValueConverter>(
                result,
            )?);
        }

        Ok(batch)
    }
}

#[allow(
    clippy::type_complexity,
    reason = "already factored through ReceiveErrorFor"
)]
fn completion_from_result<Metadata, Value, ValueConverter>(
    result: proto::ActionResult,
) -> Result<
    ActionCallCompletion<Metadata, Value, core::convert::Infallible>,
    ReceiveErrorFor<Metadata, Value, ValueConverter>,
>
where
    Metadata: Decode,
    ValueConverter: TryConvert<Vec<u8>, waymark_action_runtime_core::ActionCallOutcome<Value>>,
{
    let metadata = Metadata::decode(&mut result.metadata.as_slice()).map_err(|error| {
        tracing::error!(
            ?result,
            %error,
            "unable to decode correlation metadata for an action completion"
        );
        ReceiveError::Decode(error)
    })?;
    let outcome = ValueConverter::try_convert(result.payload).map_err(ReceiveError::Payload)?;
    Ok(ActionCallCompletion {
        metadata,
        execution_result: Ok(outcome),
    })
}

#[cfg(test)]
mod tests {
    use waymark_action_runtime_core::ActionCallCompletionsProvider as _;
    use waymark_action_runtime_metadata::ActionCallCorrelation;

    use super::*;

    #[tokio::test]
    async fn undecodable_metadata_surfaces_as_error() {
        let (tx, rx) = mpsc::channel(4);
        let mut provider = WorkerStreamActionCallCompletionsProvider::<
            ActionCallCorrelation,
            waymark_vm_value_python::ReadyValue,
            waymark_vm_value_python_convert_proto::Converter,
        >::new(rx);

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
        let mut provider = WorkerStreamActionCallCompletionsProvider::<
            ActionCallCorrelation,
            waymark_vm_value_python::ReadyValue,
            waymark_vm_value_python_convert_proto::Converter,
        >::new(rx);
        drop(tx);

        let result = provider.wait_for_completions().await;
        assert!(matches!(result, Err(ReceiveError::ChannelClosed)));
    }
}
