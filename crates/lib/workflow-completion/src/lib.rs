//! Handles core VM effects by recording workflow completion results.
//!
//! Provides an [`waymark_vm_driver_core::EffectHandler`] implementation that
//! takes [`waymark_vm_interpreter_coreset::Effect`] values directly and
//! persists completion or exception outcomes through the shared
//! [`outcome_batcher`], which coalesces them into batched
//! [`waymark_workflow_completion_backend::RecordOutcomes`] statements.

#![warn(missing_docs)]

pub mod outcome_batcher;

use std::marker::PhantomData;

use waymark_vm_interpreter_coreset::Effect as CoreSetEffect;

/// Error returned by [`EffectHandler::handle_effect`].
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<CodecError> {
    /// Serialization of the completion value or exception failed.
    #[error("codec: {0:?}")]
    Codec(#[source] CodecError),

    /// Recording the terminal outcome failed fatally — a different outcome
    /// is already stored, or the batcher is gone.
    #[error("persisting outcome: {0}")]
    Record(#[source] crate::outcome_batcher::RecordError),
}

/// An [`waymark_vm_driver_core::EffectHandler`] that records workflow
/// completion outcomes through the shared outcome batcher.
///
/// Values are serialized via the given `Codec` before being submitted.
pub struct EffectHandler<VmId, Codec, ReadyValue> {
    /// The shared recorder durably persisting terminal outcomes in batches.
    recorder: crate::outcome_batcher::OutcomeRecorderHandle<VmId>,
    vm_id: VmId,
    codec: Codec,
    // The handler never owns a `ReadyValue`; it only receives one per effect.
    // `fn() -> ReadyValue` keeps covariance without borrowing the type's
    // `Send`/`Sync`/dropck, so the handler stays `Send + Sync` regardless.
    _phantom_data: PhantomData<fn() -> ReadyValue>,
}

impl<VmId, Codec, ReadyValue> EffectHandler<VmId, Codec, ReadyValue> {
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

impl<VmId, Codec, ReadyValue> waymark_vm_driver_core::EffectHandler
    for EffectHandler<VmId, Codec, ReadyValue>
where
    VmId: Clone + Send + Sync,
    Codec: waymark_vm_codec_core::SerializerProvider + Send,
    ReadyValue: serde::Serialize + Send,
    waymark_vm_runtime_exception::Exception<ReadyValue>: serde::Serialize,
{
    type Effect = CoreSetEffect<ReadyValue>;
    type Error = HandleEffectError<Codec::Error>;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        let outcome = match emitted_effect.effect {
            CoreSetEffect::Complete(value) => {
                tracing::info!("workflow completed successfully");
                let mut buf = Vec::new();
                self.codec
                    .with_serializer(&mut buf, |ser| serde::Serialize::serialize(&value, ser))
                    .map_err(HandleEffectError::Codec)?;
                waymark_workflow_completion_backend::Outcome::Completion(buf)
            }
            CoreSetEffect::UnhandledException(exception) => {
                tracing::info!(
                    exception_type = %exception.type_id,
                    "workflow terminated with unhandled exception",
                );
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

#[cfg(test)]
mod tests {
    use super::*;

    // The handler is meant to be driven from a spawned task and shared, so it
    // must stay `Send + Sync` regardless of the `ReadyValue` type parameter —
    // which it never owns. Assert that with a deliberately `!Send + !Sync`
    // `ReadyValue` (`Rc`), the handler remains both.
    #[test]
    fn handler_is_send_sync_independent_of_ready_value() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EffectHandler<(), (), std::rc::Rc<()>>>();
    }
}
