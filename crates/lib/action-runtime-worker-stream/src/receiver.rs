use std::marker::PhantomData;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_convert_core::Convert;
use waymark_proto::messages as proto;

/// Error returned when receiving action results fails.
#[derive(Debug)]
pub enum ReceiveError {
    /// The result channel was closed.
    ChannelClosed,
}

/// Receives action results from a tokio mpsc channel and surfaces them
/// as [`waymark_action_runtime_core::ActionCallOutcome`]s.
///
/// The `ValueConverter` converts [`proto::WorkflowArguments`] payloads
/// into the provider's [`Value`] type.
pub struct ActionResultReceiver<ValueConverter> {
    rx: mpsc::Receiver<proto::ActionResult>,
    phantom_data: PhantomData<ValueConverter>,
}

impl<ValueConverter> ActionResultReceiver<ValueConverter> {
    /// Create a new receiver.
    pub fn new(rx: mpsc::Receiver<proto::ActionResult>) -> Self {
        Self {
            rx,
            phantom_data: PhantomData,
        }
    }
}

impl<ValueConverter> waymark_action_runtime_core::ActionCallOutcomesProvider
    for ActionResultReceiver<ValueConverter>
where
    ValueConverter: Convert<waymark_proto::messages::WorkflowArguments, serde_json::Value> + Send,
{
    type Value = serde_json::Value;
    type Error = ReceiveError;

    async fn wait_for_outcomes(
        &mut self,
    ) -> Result<NEVec<waymark_action_runtime_core::ActionCallOutcome<Self::Value>>, Self::Error>
    {
        let mut rx = std::mem::replace(
            &mut self.rx,
            tokio::sync::mpsc::channel::<proto::ActionResult>(1).1,
        );

        let result = rx.recv().await.ok_or(ReceiveError::ChannelClosed)?;

        let outcome = self.outcome_from_result(result);

        let mut outcomes = NEVec::new(outcome);

        while let Ok(result) = rx.try_recv() {
            outcomes.push(self.outcome_from_result(result));
        }

        self.rx = rx;

        Ok(outcomes)
    }
}

impl<ValueConverter> ActionResultReceiver<ValueConverter>
where
    ValueConverter: Convert<waymark_proto::messages::WorkflowArguments, serde_json::Value>,
{
    fn outcome_from_result(
        &self,
        result: proto::ActionResult,
    ) -> waymark_action_runtime_core::ActionCallOutcome<serde_json::Value> {
        if result.success {
            let value = match result.payload {
                Some(payload) => ValueConverter::convert(payload),
                None => serde_json::Value::Null,
            };
            waymark_action_runtime_core::ActionCallOutcome::Value(value)
        } else {
            let error_message = result.error_message.unwrap_or_default();
            let error_type = result.error_type.unwrap_or_else(|| "ActionError".into());
            let details = serde_json::json!({
                "type": &error_type,
                "message": &error_message,
            });
            let exception = waymark_vm_runtime_exception::Exception {
                type_id: error_type,
                details,
            };
            waymark_action_runtime_core::ActionCallOutcome::Exception(exception)
        }
    }
}
