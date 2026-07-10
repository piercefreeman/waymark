use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};
use waymark_action_runtime_metadata::ActionCallCorrelation;
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
    type Metadata = ActionCallCorrelation;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        loop {
            let result = self.rx.recv().await.ok_or(ReceiveError::ChannelClosed)?;

            let mut batch = Vec::new();

            if let Some(completion) = completion_from_result(&result) {
                batch.push(completion);
            }

            while let Ok(result) = self.rx.try_recv() {
                if let Some(completion) = completion_from_result(&result) {
                    batch.push(completion);
                }
            }

            let Some(batch) = NEVec::try_from_vec(batch) else {
                continue;
            };

            return Ok(batch);
        }
    }
}

fn completion_from_result(
    result: &proto::ActionResult,
) -> Option<ActionCallCompletion<waymark_vm_value::ReadyValue, ActionCallCorrelation>> {
    let outcome = waymark_action_runtime_convert::Converter::convert(result);
    let Some((effect_number, promise_state_id)) = parse_action_id(&result.action_id) else {
        tracing::warn!(
            ?result,
            "unable to parse correlation metadata for an action completion"
        );
        return None;
    };
    Some(ActionCallCompletion {
        metadata: ActionCallCorrelation {
            effect_number,
            promise_state_id,
        },
        outcome,
    })
}

/// Parse an action ID string of the form `"{effect_number}/PromiseStateId({id})"`.
fn parse_action_id(action_id: &str) -> Option<(EffectNumber, PromiseStateId)> {
    // Find the "/PromiseStateId(" separator
    let prefix = "/PromiseStateId(";
    let sep = action_id.find(prefix)?;

    let effect_str = &action_id[..sep];
    let effect_number = effect_str.parse::<usize>().map(EffectNumber).ok()?;

    let start = sep + prefix.len();
    let end = action_id[start..].find(')')?;
    let id_str = &action_id[start..start + end];
    let promise_state_id = id_str.parse::<usize>().map(PromiseStateId).ok()?;

    Some((effect_number, promise_state_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_action_id() {
        assert_eq!(
            parse_action_id("0/PromiseStateId(0)"),
            Some((EffectNumber(0), PromiseStateId(0)))
        );
        assert_eq!(
            parse_action_id("1/PromiseStateId(1)"),
            Some((EffectNumber(1), PromiseStateId(1)))
        );
        assert_eq!(
            parse_action_id("42/PromiseStateId(99)"),
            Some((EffectNumber(42), PromiseStateId(99)))
        );
    }

    #[test]
    fn test_parse_action_id_invalid() {
        assert_eq!(parse_action_id(""), None);
        assert_eq!(parse_action_id("not-an-action-id"), None);
        assert_eq!(parse_action_id("0/PromiseStateId()"), None);
        assert_eq!(parse_action_id("0/PromiseStateId(abc)"), None);
    }
}
