//! Durable writer — consumes action-call completions from a provider and
//! records them durably.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use nonempty_collections::NEVec;
use waymark_action_completions_reconciler_backend::record_completions::{
    Error as _, ErrorKind, RecordingSuccess,
};
use waymark_action_completions_reconciler_backend::{CompletionRecord, RecordCompletions};
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionsProvider};
use waymark_action_runtime_metadata::{ActionCallCorrelated, VmScoped};

/// Initial delay between retries of a failed (retryable) record operation.
const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(25);

/// Cap on the retry delay.
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Error returned when the writer stops.
///
/// Every variant is critical: the caller should treat the writer's death
/// as fatal for the execution subsystem (drop-guard escalation).
/// Retryable backend failures are retried internally and never surface
/// here.
#[derive(Debug, thiserror::Error)]
pub enum Error<ProviderError, EncodeError, RecordError> {
    /// The completions provider failed; no further completions can be
    /// ingested.
    #[error("waiting for action-call completions: {0}")]
    Completions(#[source] ProviderError),

    /// An action-call outcome could not be encoded for storage.
    #[error("unable to encode an action-call outcome")]
    OutcomeEncode(#[source] EncodeError),

    /// The backend reported diverging effect numbers — the
    /// "same effect ⇒ same pair" invariant is broken.
    #[error("diverging effect numbers reported by the backend")]
    DivergentEffectNumber(#[source] RecordError),
}

/// The [`Error`] type produced by [`run`] over the given provider,
/// backend, and codec.
type RunError<Provider, Backend, Codec> = Error<
    <Provider as ActionCallCompletionsProvider>::Error,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error,
    <Backend as RecordCompletions>::Error,
>;

/// Parameters for [`run`].
pub struct Params<Provider, Backend, Codec> {
    /// The completion source to ingest from — typically the worker-pool
    /// provider, but anything implementing
    /// [`ActionCallCompletionsProvider`] whose metadata carries the VM id
    /// and call correlation works.
    pub provider: Provider,

    /// The durable completions backend to record into.
    pub backend: Arc<Backend>,

    /// The codec used to encode action-call outcomes for storage.
    pub codec: Codec,
}

/// Ingest and record completions until the provider fails.
///
/// Drive this in a background task.  The loop never completes normally —
/// it only ends on an error, every one of which is critical; see
/// [`Error`].
pub async fn run<Provider, Backend, Codec>(
    params: Params<Provider, Backend, Codec>,
) -> Result<std::convert::Infallible, RunError<Provider, Backend, Codec>>
where
    Provider: ActionCallCompletionsProvider,
    Provider::Metadata: VmScoped<VmId = Backend::VmId> + ActionCallCorrelated,
    Provider::Value: serde::Serialize,
    Backend: RecordCompletions,
    Codec: waymark_vm_codec_core::SerializerProvider,
{
    let Params {
        mut provider,
        backend,
        codec,
    } = params;

    loop {
        let completions = provider
            .wait_for_completions()
            .await
            .map_err(Error::Completions)?;

        let mut records = Vec::with_capacity(completions.len().get());
        for completion in completions {
            let ActionCallCompletion { metadata, outcome } = completion;

            let mut blob = Vec::new();
            codec
                .with_serializer(&mut blob, |serializer| {
                    serde::Serialize::serialize(&outcome, serializer)
                })
                .map_err(Error::OutcomeEncode)?;

            // The record is keyed by the metadata the provider recovered.
            let correlation = metadata.call_correlation();
            records.push(CompletionRecord {
                vm_id: metadata.vm_id(),
                promise_state_id: correlation.promise_state_id,
                effect_number: correlation.effect_number,
                outcome: blob,
            });
        }
        let records = NEVec::try_from_vec(records).expect("a non-empty batch resolves non-empty");

        record_with_retry(&*backend, &records)
            .await
            .map_err(Error::DivergentEffectNumber)?;
    }
}

/// Record a batch, retrying retryable failures indefinitely.
///
/// Returns an error only on divergence — the one failure that must never
/// be retried.
async fn record_with_retry<Backend>(
    backend: &Backend,
    records: &NEVec<CompletionRecord<Backend::VmId>>,
) -> Result<(), Backend::Error>
where
    Backend: RecordCompletions,
{
    let mut backoff = RETRY_INITIAL_BACKOFF;
    loop {
        match backend
            .record_completions(records.as_nonempty_slice())
            .await
        {
            Ok(RecordingSuccess::AllRecorded) => return Ok(()),
            Ok(RecordingSuccess::SomeConflictingOutcomes(keys)) => {
                // At-least-once redelivery of a non-deterministic retry;
                // the first recorded outcome wins.
                tracing::error!(
                    conflicting = keys.len().get(),
                    "conflicting outcomes for already-recorded completions; \
                     first write wins"
                );
                return Ok(());
            }
            Err(error) => match error.kind() {
                ErrorKind::Internal => {
                    tracing::warn!(?error, ?backoff, "recording completions failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RETRY_MAX_BACKOFF);
                }
                ErrorKind::DivergentEffectNumber => {
                    return Err(error);
                }
            },
        }
    }
}
