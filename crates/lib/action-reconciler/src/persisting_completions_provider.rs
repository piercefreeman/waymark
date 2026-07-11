//! A completions-provider decorator that records outcomes durably before
//! delivering them.

use std::sync::Arc;

use nonempty_collections::NEVec;
use waymark_action_runtime_core::{
    ActionCallCompletionFor, ActionCallCompletionsProvider, ActionCallOutcome,
};
use waymark_action_runtime_metadata::{ActionCallCorrelated as _, VmScoped as _};

/// Decorates an
/// [`ActionCallCompletionsProvider`](waymark_action_runtime_core::ActionCallCompletionsProvider),
/// recording each completion's outcome onto its pending-call record before
/// yielding the batch downstream.
///
/// Sits between the raw completions source (e.g. the worker pool) and the
/// per-VM routing, so an outcome is durable before any VM can observe it:
/// once recorded, the completion survives the executing process, and a
/// revived VM settles from the record instead of re-executing the action.
///
/// Recording is best-effort: a completion whose outcome cannot be recorded
/// is still delivered, keeping the pipeline live — if that delivery is then
/// lost too, recovery falls back to re-dispatching the call on revive.
pub struct PersistingCompletionsProvider<Inner, Backend, Codec> {
    inner: Inner,
    backend: Arc<Backend>,
    codec: Codec,
}

impl<Inner, Backend, Codec> PersistingCompletionsProvider<Inner, Backend, Codec> {
    /// Create a new persisting decorator around `inner`.
    pub fn new(inner: Inner, backend: Arc<Backend>, codec: Codec) -> Self {
        Self {
            inner,
            backend,
            codec,
        }
    }
}

impl<Inner, Backend, Codec> ActionCallCompletionsProvider
    for PersistingCompletionsProvider<Inner, Backend, Codec>
where
    Inner: ActionCallCompletionsProvider + Send + Sync,
    Inner::Metadata: waymark_action_runtime_metadata::VmScoped
        + waymark_action_runtime_metadata::ActionCallCorrelated
        + Send
        + Sync,
    Inner::Value: serde::Serialize + Send + Sync,
    waymark_vm_runtime_exception::Exception<Inner::Value>: serde::Serialize,
    Backend:
        waymark_action_reconciler_backend::StoreActionCallOutcome<VmId = waymark_ids::InstanceId>,
    Backend: Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider + Send + Sync,
    Codec::Error: Send,
{
    type Value = Inner::Value;
    type Error = Inner::Error;
    type Metadata = Inner::Metadata;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error> {
        let batch = self.inner.wait_for_completions().await?;

        for completion in &batch {
            self.record_outcome(completion).await;
        }

        Ok(batch)
    }
}

impl<Inner, Backend, Codec> PersistingCompletionsProvider<Inner, Backend, Codec>
where
    Inner: ActionCallCompletionsProvider,
    Inner::Metadata: waymark_action_runtime_metadata::VmScoped
        + waymark_action_runtime_metadata::ActionCallCorrelated,
    Inner::Value: serde::Serialize,
    waymark_vm_runtime_exception::Exception<Inner::Value>: serde::Serialize,
    Backend:
        waymark_action_reconciler_backend::StoreActionCallOutcome<VmId = waymark_ids::InstanceId>,
    Codec: waymark_vm_codec_core::SerializerProvider,
{
    /// Record one completion's outcome onto its pending-call record.
    async fn record_outcome(&self, completion: &ActionCallCompletionFor<Inner>) {
        use waymark_action_reconciler_backend::{
            PendingActionCallOutcome, StoreActionCallOutcomeStatus,
        };

        let vm_id = completion.metadata.vm_id();
        let promise_state_id = completion.metadata.call_correlation().promise_state_id;

        let mut bytes = Vec::new();
        let encoded = match &completion.outcome {
            ActionCallOutcome::Value(value) => self
                .codec
                .with_serializer(&mut bytes, |serializer| {
                    serde::Serialize::serialize(value, serializer)
                })
                .map(|_| PendingActionCallOutcome::Value(bytes)),
            ActionCallOutcome::Exception(exception) => self
                .codec
                .with_serializer(&mut bytes, |serializer| {
                    serde::Serialize::serialize(exception, serializer)
                })
                .map(|_| PendingActionCallOutcome::Exception(bytes)),
        };
        let outcome = match encoded {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    %vm_id,
                    ?promise_state_id,
                    "unable to encode an action call outcome; delivering it non-durably"
                );
                return;
            }
        };

        let stored = self
            .backend
            .store_action_call_outcome(&vm_id, promise_state_id, outcome)
            .await;
        match stored {
            Ok(StoreActionCallOutcomeStatus::Stored) => {}
            Ok(StoreActionCallOutcomeStatus::NotPending) => {
                // Duplicate delivery, or the call already settled and its
                // record was removed — first write wins either way.
                tracing::debug!(
                    %vm_id,
                    ?promise_state_id,
                    "action call outcome not recorded: no record awaiting one"
                );
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    %vm_id,
                    ?promise_state_id,
                    "unable to record an action call outcome; delivering it non-durably"
                );
            }
        }
    }
}
