//! Handles terminal VM effects by recording workflow completion results.
//!
//! Provides an [`waymark_vm_driver_core::EffectHandler`] implementation that
//! takes [`waymark_fullset_effect_handler::TerminalEffect`] values directly
//! and persists completion or exception outcomes through the shared
//! [`outcome_batcher`], which coalesces them into batched
//! [`waymark_workflow_completion_backend::RecordOutcomes`] statements.

#![warn(missing_docs)]

pub mod outcome_batcher;

#[cfg(test)]
mod tests;

use std::marker::PhantomData;

use waymark_fullset_effect_handler::TerminalEffect;

/// Error returned by [`EffectHandler`]'s [`handle_effect`](waymark_vm_driver_core::EffectHandler::handle_effect).
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<CodecError> {
    /// Serialization of the completion value or exception failed.
    #[error("codec: {0:?}")]
    Codec(#[source] CodecError),

    /// Recording the terminal outcome failed fatally — a different outcome
    /// is already stored, the recording failed beyond retry, or the
    /// batcher is gone.
    #[error("persisting outcome: {0}")]
    Record(#[source] crate::outcome_batcher::RecordError),
}

/// An [`waymark_vm_driver_core::EffectHandler`] that records workflow
/// completion outcomes through the shared outcome batcher.
///
/// Values are serialized via the given `Codec` before being submitted.
pub struct EffectHandler<VmId, Codec, ReadyValue, RaisedException> {
    /// The shared recorder durably persisting terminal outcomes in batches.
    recorder: crate::outcome_batcher::OutcomeRecorderHandle<VmId>,
    vm_id: VmId,
    codec: Codec,
    // The handler never owns a `ReadyValue` or a `RaisedException`; it only
    // receives one per effect. `fn() -> (ReadyValue, RaisedException)` keeps
    // covariance without borrowing the types' `Send`/`Sync`/dropck, so the
    // handler stays `Send + Sync` regardless.
    _phantom_data: PhantomData<fn() -> (ReadyValue, RaisedException)>,
}

impl<VmId, Codec, ReadyValue, RaisedException>
    EffectHandler<VmId, Codec, ReadyValue, RaisedException>
{
    /// Create a new handler.
    pub fn new(
        recorder: crate::outcome_batcher::OutcomeRecorderHandle<VmId>,
        vm_id: VmId,
        codec: Codec,
    ) -> Self {
        Self {
            recorder,
            vm_id,
            codec,
            _phantom_data: PhantomData,
        }
    }
}

impl<VmId, Codec, ReadyValue, RaisedException> waymark_vm_driver_core::EffectHandler
    for EffectHandler<VmId, Codec, ReadyValue, RaisedException>
where
    VmId: Clone + Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider + Send,
    ReadyValue: serde::Serialize + Send,
    RaisedException: serde::Serialize + core::fmt::Debug + Send,
{
    type Effect = TerminalEffect<ReadyValue, RaisedException>;
    type Error = HandleEffectError<Codec::Error>;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        let outcome = match emitted_effect.effect {
            TerminalEffect::Complete(value) => {
                tracing::debug!("workflow completed successfully");
                let mut buf = Vec::new();
                self.codec
                    .with_serializer(&mut buf, |ser| serde::Serialize::serialize(&value, ser))
                    .map_err(HandleEffectError::Codec)?;
                waymark_workflow_completion_backend::Outcome::Completion(buf)
            }
            TerminalEffect::UnhandledException(exception) => {
                tracing::debug!(?exception, "workflow terminated with unhandled exception");
                let mut buf = Vec::new();
                self.codec
                    .with_serializer(&mut buf, |ser| serde::Serialize::serialize(&exception, ser))
                    .map_err(HandleEffectError::Codec)?;
                waymark_workflow_completion_backend::Outcome::Exception(buf)
            }
        };

        match self.recorder.submit((self.vm_id.clone(), outcome)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(HandleEffectError::Record(error)),
            Err(waymark_batcher::Closed) => Err(HandleEffectError::Record(
                crate::outcome_batcher::RecordError::Closed,
            )),
        }
    }
}
