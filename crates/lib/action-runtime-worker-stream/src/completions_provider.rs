use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::ActionCallCompletion;
use waymark_convert_core::Convert as _;
use waymark_proto::messages as proto;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Error returned when receiving action results fails.
#[derive(Debug)]
pub enum ReceiveError {
    /// The result channel was closed.
    ChannelClosed,
}

/// Receives action results from a tokio mpsc channel and surfaces them
/// as [`waymark_action_runtime_core::ActionCallCompletion`]s.
pub struct WorkerStreamActionCallCompletionsProvider {
    /// The receiver of the action results.
    pub rx: mpsc::Receiver<proto::ActionResult>,
}

impl waymark_action_runtime_core::ActionCallCompletionsProvider
    for WorkerStreamActionCallCompletionsProvider
{
    type Value = waymark_vm_value::ReadyValue;
    type Error = ReceiveError;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletion<Self::Value>>, Self::Error> {
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

fn completion_from_result(
    result: &proto::ActionResult,
) -> ActionCallCompletion<waymark_vm_value::ReadyValue> {
    let outcome = waymark_action_runtime_convert::Converter::convert(result);
    // TODO: fix effect number and promise state id
    ActionCallCompletion {
        effect_number: EffectNumber(0),
        promise_state_id: PromiseStateId(0),
        outcome,
    }
}
