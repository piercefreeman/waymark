//! The per-VM durable action effect handler.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use waymark_action_core::ActionRef;
use waymark_action_effect_reconciler_backend::record_action_call_requests::RecordingSuccess;
use waymark_action_effect_reconciler_backend::{
    ActionCallRequestKey, ActionCallRequestRecord, HasLockOwnerId, HasTimestamp, HasVmId,
    RecordActionCallRequests,
};
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata::ActionCallCorrelation;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::action_call_request_payload::ActionCallRequestPayload;
use crate::issuance::{record_with_retry, track_for_renewal};
use crate::renewal::HeldLock;

/// Error returned when handling an action-call effect fails.
///
/// Every variant is critical to the emitting VM's drive loop; retryable
/// backend failures are retried internally and never surface here.
#[derive(Debug, thiserror::Error)]
pub enum Error<EncodeError, RecordError, DeliverError> {
    /// The request payload could not be encoded for storage.
    #[error("unable to encode an action-call request payload")]
    Encode(#[source] EncodeError),

    /// The backend rejected the record — the key already exists with a
    /// different payload, i.e. replay determinism is broken.
    #[error("recording an action-call request: {0}")]
    Record(#[source] RecordError),

    /// The local pool rejected the delivery.
    #[error("delivering an action call: {0}")]
    Deliver(#[source] DeliverError),
}

/// Handles action-call effects durably: store-before-deliver.
///
/// Implements [`waymark_extcall_reconciler_core::ActionEffectHandler`]
/// for one VM.  Each effect is recorded as a born-locked request row and
/// then delivered to the local worker pool through the held requester; a
/// replayed effect is recognized by its key and not delivered again —
/// its delivery was already decided (by the revival reconcile, or by the
/// owner still running it).
pub struct EffectHandler<Backend, Codec, ActionCallRequester>
where
    Backend: HasVmId + HasLockOwnerId,
{
    /// The durable requests backend.
    pub backend: Arc<Backend>,

    /// The codec used to encode request payloads.
    pub codec: Codec,

    /// The identity of this process as a lock owner.
    pub lock_owner_id: Backend::LockOwnerId,

    /// How long a request lock lasts before it needs to be renewed.
    pub lock_time_to_live: NonZeroDuration,

    /// Locks taken for delivered calls, feeding the renewal loop.
    pub held_locks_tx: tokio::sync::mpsc::UnboundedSender<HeldLock<Backend::VmId>>,

    /// The VM this handler serves.
    pub vm_id: Backend::VmId,

    /// The requester delivering calls to the local worker pool.
    pub requester: ActionCallRequester,
}

impl<Backend, Codec, ActionCallRequester> waymark_extcall_reconciler_core::ActionEffectHandler
    for EffectHandler<Backend, Codec, ActionCallRequester>
where
    Backend: HasVmId + HasLockOwnerId + HasTimestamp<Timestamp = DateTime<Utc>>,
    Backend: RecordActionCallRequests + Send + Sync,
    <Backend as RecordActionCallRequests>::Error: Send,
    Backend::VmId: Clone + Send + Sync,
    Backend::LockOwnerId: Clone + Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider + Send + Sync,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error: Send,
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>
        + Send
        + Sync,
    ActionCallRequester::Argument: serde::Serialize + Send + Sync,
{
    type Error = Error<
        <Codec as waymark_vm_codec_core::SerializerProvider>::Error,
        <Backend as RecordActionCallRequests>::Error,
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
        let (success, taken_at) = record_with_retry(
            &*self.backend,
            &self.lock_owner_id,
            self.lock_time_to_live,
            &record,
        )
        .await
        .map_err(Error::Record)?;

        if let RecordingSuccess::SomeAlreadyRecorded(_) = success {
            tracing::debug!(
                ?promise_state_id,
                "request already recorded (effect replay); not delivering"
            );
            return Ok(());
        }

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

        track_for_renewal(
            &self.held_locks_tx,
            HeldLock {
                key: ActionCallRequestKey {
                    vm_id: self.vm_id.clone(),
                    promise_state_id,
                },
                taken_at,
            },
        );
        Ok(())
    }
}
