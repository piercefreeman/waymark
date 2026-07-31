//! The spawning-factory decorator that reconciles a VM's pending
//! requests before the VM is produced.

#[cfg(test)]
mod tests;

use waymark_action_effect_reconciler_backend::ActionCallRequestKey;
use waymark_action_runtime_core::ActionCallRequest;
use waymark_action_runtime_metadata::ActionCallCorrelation;

use crate::action_call_request_payload::ActionCallRequestPayload;
use crate::issuance::track_for_renewal;
use crate::lock_batcher::{LockError, VmLockerHandle};
use crate::renewal::HeldLock;

/// Error returned when producing a VM through the reconciling factory
/// fails.
#[derive(Debug, thiserror::Error)]
pub enum Error<Reconcile, Inner> {
    /// Reconciling the VM's pending action-call requests failed.
    #[error("reconciling pending action-call requests: {0}")]
    Reconcile(#[source] Reconcile),

    /// The inner factory failed to produce the VM.
    #[error(transparent)]
    Inner(Inner),
}

/// Error returned when reconciling a VM's pending requests fails.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileVmError<DecodeError, DeliverError> {
    /// Locking the VM's pending requests failed.
    #[error("locking pending action-call requests: {0}")]
    Lock(#[source] LockError),

    /// A stored request payload could not be decoded.
    #[error("unable to decode a stored action-call request payload")]
    Decode(#[source] DecodeError),

    /// The local pool rejected a redelivery.
    #[error("redelivering an action call: {0}")]
    Deliver(#[source] DeliverError),
}

/// A [`waymark_state_manager_core::Factory`] decorator: reconciles the
/// VM's pending action-call requests, then produces the VM.
///
/// The reconcile locks every eligible request row of the VM (unlocked,
/// or lock expired — the owner died) through the shared lock batcher,
/// coalesced with other concurrently-revived VMs, and redelivers its
/// calls to the local pool; rows held by another live owner are left
/// alone, an attempt is presumed running in that owner's pool.
///
/// `produce` runs exactly once per real spawn (never on a retrieval), so
/// the reconcile completes before the VM exists — the dangerous order
/// (VM driving before reconcile) is impossible by construction.  A
/// reconcile failure surfaces as a spawn failure: the VM stays unpinned
/// and the next pinning cycle retries.
pub struct ReconcilingFactory<Inner, VmId, Codec, RequesterProvider> {
    /// The factory producing the VM once the world is consistent.
    pub inner: Inner,

    /// The shared batcher locking pending requests at revival.
    pub locker: VmLockerHandle<VmId>,

    /// The codec used to decode stored request payloads.
    pub codec: Codec,

    /// Locks taken for redelivered calls, feeding the renewal loop.
    pub held_locks_tx: tokio::sync::mpsc::UnboundedSender<HeldLock<VmId>>,

    /// Mints the per-VM requester the redeliveries go through.
    pub requester_provider: RequesterProvider,
}

impl<Inner, VmId, Codec, RequesterProvider, ActionCallRequester> waymark_state_manager_core::Factory
    for ReconcilingFactory<Inner, VmId, Codec, RequesterProvider>
where
    Inner: waymark_state_manager_core::Factory<Key = VmId> + Send + Sync,
    VmId: Clone + Send + Sync + core::fmt::Debug,
    Codec: waymark_vm_codec_core::DeserializerProvider + Send + Sync,
    <Codec as waymark_vm_codec_core::DeserializerProvider>::Error: Send,
    RequesterProvider: Fn(&VmId) -> ActionCallRequester + Send + Sync,
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<Metadata = ActionCallCorrelation>
        + Send
        + Sync
        + 'static,
    ActionCallRequester::Argument: serde::de::DeserializeOwned + Send + Sync,
{
    type Key = Inner::Key;
    type Value = Inner::Value;
    type Error = Error<
        ReconcileVmError<
            <Codec as waymark_vm_codec_core::DeserializerProvider>::Error,
            ActionCallRequester::Error,
        >,
        Inner::Error,
    >;

    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        let (outcome, taken_at) = match self.locker.submit(key.clone()).await {
            Ok(Ok(pair)) => pair,
            Ok(Err(error)) => {
                return Err(Error::Reconcile(ReconcileVmError::Lock(error)));
            }
            Err(waymark_batcher::Closed) => {
                return Err(Error::Reconcile(ReconcileVmError::Lock(LockError::Closed)));
            }
        };

        if !outcome.held_elsewhere.is_empty() {
            tracing::debug!(
                vm_id = ?key,
                held_elsewhere = outcome.held_elsewhere.len(),
                "requests held by another live owner; leaving them alone"
            );
        }

        let requester = (self.requester_provider)(key);
        for record in outcome.locked {
            let payload: ActionCallRequestPayload<ActionCallRequester::Argument> = self
                .codec
                .with_deserializer(&record.request, |deserializer| {
                    serde::Deserialize::deserialize(deserializer)
                })
                .map_err(ReconcileVmError::Decode)
                .map_err(Error::Reconcile)?;

            requester
                .request_action_call(ActionCallRequest {
                    action_ref: payload.action_ref,
                    arguments: payload.arguments,
                    metadata: ActionCallCorrelation {
                        effect_number: record.effect_number,
                        promise_state_id: record.promise_state_id,
                    },
                })
                .await
                .map_err(ReconcileVmError::Deliver)
                .map_err(Error::Reconcile)?;

            track_for_renewal(
                &self.held_locks_tx,
                HeldLock {
                    key: ActionCallRequestKey {
                        vm_id: record.vm_id,
                        promise_state_id: record.promise_state_id,
                    },
                    taken_at,
                },
            );
        }

        self.inner.produce(key).await.map_err(Error::Inner)
    }
}
