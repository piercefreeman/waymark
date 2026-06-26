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
    let (effect_number, promise_state_id) = parse_action_id(&result.action_id);
    ActionCallCompletion {
        effect_number,
        promise_state_id,
        outcome,
    }
}

/// Parse an action ID string of the form `"{effect_number}/PromiseStateId({id})"`.
///
/// Returns `(EffectNumber(0), PromiseStateId(0))` when parsing fails.
fn parse_action_id(action_id: &str) -> (EffectNumber, PromiseStateId) {
    // Find the "/PromiseStateId(" separator
    let prefix = "/PromiseStateId(";
    let sep = match action_id.find(prefix) {
        Some(pos) => pos,
        None => return (EffectNumber(0), PromiseStateId(0)),
    };

    let effect_str = &action_id[..sep];
    let effect_number = effect_str
        .parse::<usize>()
        .map(EffectNumber)
        .unwrap_or(EffectNumber(0));

    let start = sep + prefix.len();
    let end = match action_id[start..].find(')') {
        Some(pos) => pos,
        None => return (effect_number, PromiseStateId(0)),
    };
    let id_str = &action_id[start..start + end];
    let promise_state_id = id_str
        .parse::<usize>()
        .map(PromiseStateId)
        .unwrap_or(PromiseStateId(0));

    (effect_number, promise_state_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_action_id() {
        assert_eq!(
            parse_action_id("0/PromiseStateId(0)"),
            (EffectNumber(0), PromiseStateId(0))
        );
        assert_eq!(
            parse_action_id("1/PromiseStateId(1)"),
            (EffectNumber(1), PromiseStateId(1))
        );
        assert_eq!(
            parse_action_id("42/PromiseStateId(99)"),
            (EffectNumber(42), PromiseStateId(99))
        );
    }

    #[test]
    fn test_parse_action_id_invalid() {
        assert_eq!(parse_action_id(""), (EffectNumber(0), PromiseStateId(0)));
        assert_eq!(
            parse_action_id("not-an-action-id"),
            (EffectNumber(0), PromiseStateId(0))
        );
        assert_eq!(
            parse_action_id("0/PromiseStateId()"),
            (EffectNumber(0), PromiseStateId(0))
        );
        assert_eq!(
            parse_action_id("0/PromiseStateId(abc)"),
            (EffectNumber(0), PromiseStateId(0))
        );
    }
}
