//! The per-VM durable action effect handler.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use waymark_action_core::ActionRef;
use waymark_action_effect_reconciler_backend::ActionCallRequestRecord;
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::action_call_request_locks::Tracker;
use crate::action_call_request_payload::ActionCallRequestPayload;
use crate::request_batcher::{RecordError, RecordOutcome, RequestRecorderHandle};

/// Error returned when handling an action-call effect fails.
///
/// Every variant is critical to the emitting VM's drive loop; retryable
/// backend failures are retried inside the request batcher and never
/// surface here.
#[derive(Debug, thiserror::Error)]
pub enum Error<EncodeError, DeliverError> {
    /// The request payload could not be encoded for storage.
    #[error("unable to encode an action-call request payload: {0}")]
    Encode(#[source] EncodeError),

    /// Recording the request failed fatally — the key diverged or the
    /// batcher is gone.
    #[error("recording an action-call request: {0}")]
    Record(#[source] RecordError),

    /// The local pool rejected the delivery.
    #[error("delivering an action call: {0}")]
    Deliver(#[source] DeliverError),
}

/// Handles action-call effects durably: store-before-deliver.
///
/// Implements [`waymark_extcall_reconciler_core::ActionEffectHandler`]
/// for one VM.  Each effect is submitted to the shared request batcher
/// (which persists it born-locked, coalesced with other VMs' requests)
/// and then delivered to the local worker pool through the held
/// requester; a replayed effect is recognized by its key and not
/// delivered again — its delivery was already decided (by adoption, or
/// by the owner still running it).  A recording whose statement committed
/// but reported failure gets the same answer on its retry, so an effect
/// found already recorded also requests a
/// [`recheck`](crate::adopting_action_settler::recheck) from the VM's
/// adopter.
pub struct EffectHandler<VmId, Codec, ActionCallRequester>
where
    VmId: Eq + std::hash::Hash,
{
    /// The shared recorder durably persisting emitted requests in batches.
    pub recorder: RequestRecorderHandle<VmId>,

    /// The codec used to encode request payloads.
    pub codec: Codec,

    /// The tracker of this process's action-call request locks, given
    /// each requested call's lock.
    pub action_call_request_locks_tracker: Arc<std::sync::Mutex<Tracker<VmId>>>,

    /// The VM this handler serves.
    pub vm_id: VmId,

    /// The requester delivering calls to the local worker pool.
    pub requester: ActionCallRequester,

    /// The VM's adopter's recheck channel; a recheck is requested when an
    /// effect is found already recorded.
    pub recheck: crate::adopting_action_settler::recheck::Tx,
}

impl<VmId, Codec, ActionCallRequester> waymark_extcall_reconciler_core::ActionEffectHandler
    for EffectHandler<VmId, Codec, ActionCallRequester>
where
    VmId: Clone + Eq + std::hash::Hash + Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider + Send + Sync,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error: Send,
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>
        + Send
        + Sync,
    ActionCallRequester::Argument: serde::Serialize + Send + Sync,
{
    type Error = Error<
        <Codec as waymark_vm_codec_core::SerializerProvider>::Error,
        ActionCallRequester::Error,
    >;
    type Argument = ActionCallRequester::Argument;

    async fn request_action(
        &mut self,
        effect_number: EffectNumber,
        promise_state_id: PromiseStateId,
        action_ref: ActionRef,
        arguments: Vec<Self::Argument>,
    ) -> Result<(), Self::Error> {
        let payload = ActionCallRequestPayload {
            action_ref,
            arguments,
        };
        let mut blob = Vec::new();
        self.codec
            .with_serializer(&mut blob, |serializer| {
                serde::Serialize::serialize(&payload, serializer)
            })
            .map_err(Error::Encode)?;

        let record = ActionCallRequestRecord {
            vm_id: self.vm_id.clone(),
            promise_state_id,
            effect_number,
            request: blob,
        };

        let outcome = match self.recorder.submit(record).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => return Err(Error::Record(error)),
            Err(waymark_batcher::Closed) => return Err(Error::Record(RecordError::Closed)),
        };

        let pending_call_request = match outcome {
            RecordOutcome::Recorded {
                pending_call_request,
            } => pending_call_request,
            RecordOutcome::AlreadyRecorded => {
                tracing::debug!(
                    ?promise_state_id,
                    "request already recorded; not delivering, the adopter rechecks"
                );
                self.recheck.request_recheck();
                return Ok(());
            }
        };

        let ActionCallRequestPayload {
            action_ref,
            arguments,
        } = payload;
        self.requester
            .request_action_call(ActionCallRequest {
                action_ref,
                arguments,
                metadata: ActionCallCorrelation {
                    effect_number,
                    promise_state_id,
                },
            })
            .await
            .map_err(Error::Deliver)?;

        pending_call_request.confirm_requested();

        Ok(())
    }
}
