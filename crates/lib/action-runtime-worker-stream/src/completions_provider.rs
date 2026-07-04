use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};
use waymark_convert_core::Convert as _;
use waymark_proto::messages as proto;

/// Error returned when receiving action results fails.
#[derive(Debug)]
pub enum ReceiveError {
    /// The result channel was closed.
    ChannelClosed,
}

/// Receives action results from a tokio mpsc channel and surfaces them
/// as [`waymark_action_runtime_core::ActionCallCompletion`]s.
pub struct WorkerStreamActionCallCompletionsProvider<Metadata> {
    /// The receiver of the action results.
    pub rx: mpsc::Receiver<proto::ActionResult>,

    /// The associated phantom data.
    pub phantom_data: std::marker::PhantomData<Metadata>,
}

impl<Metadata> WorkerStreamActionCallCompletionsProvider<Metadata> {
    /// Creates a new [`WorkerStreamActionCallCompletionsProvider`] from the given receiver.
    pub fn new(rx: mpsc::Receiver<proto::ActionResult>) -> Self {
        Self {
            rx,
            phantom_data: std::marker::PhantomData,
        }
    }
}

impl<Metadata> waymark_action_runtime_core::WithActionCallMetadata
    for WorkerStreamActionCallCompletionsProvider<Metadata>
{
    type ActionCallMetadata = Metadata;
}

impl<Metadata> waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerStreamActionCallCompletionsProvider<Metadata>
where
    Metadata: for<'a> From<&'a proto::ActionResult> + Send,
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = ReceiveError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        let mut rx = std::mem::replace(
            &mut self.rx,
            tokio::sync::mpsc::channel::<proto::ActionResult>(1).1,
        );

        let result = rx.recv().await.ok_or(ReceiveError::ChannelClosed)?;

        let outcome = completion_from_result(&result);

        let mut completions = NEVec::new(outcome);

        while let Ok(result) = rx.try_recv() {
            completions.push(completion_from_result(&result));
        }

        self.rx = rx;

        Ok(completions)
    }
}

fn completion_from_result<Metadata>(
    result: &proto::ActionResult,
) -> ActionCallCompletion<waymark_vm_value::ReadyValue, Metadata>
where
    Metadata: for<'a> From<&'a proto::ActionResult>,
{
    let metadata = Metadata::from(result);
    let outcome = waymark_action_runtime_convert::Converter::convert(result);
    ActionCallCompletion { outcome, metadata }
}
