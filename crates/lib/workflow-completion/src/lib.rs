//! Handles core VM effects by recording workflow completion results.
//!
//! Provides an [`waymark_vm_driver_core::EffectHandler`] implementation that
//! takes [`waymark_vm_interpreter_coreset::Effect`] values directly and
//! persists completion or exception outcomes via
//! [`waymark_workflow_completion_backend::RecordCompletion`] and
//! [`waymark_workflow_completion_backend::RecordException`].

#![warn(missing_docs)]

use std::marker::PhantomData;
use std::sync::Arc;

use waymark_vm_interpreter_coreset::Effect as CoreSetEffect;

/// Error returned by [`EffectHandler::handle_effect`].
#[derive(Debug, thiserror::Error)]
pub enum HandleEffectError<CodecError, CompletionError, ExceptionError> {
    /// Serialization of the completion value or exception failed.
    #[error("codec: {0:?}")]
    Codec(#[source] CodecError),

    /// The backend failed to record a completion.
    #[error("persisting completion: {0}")]
    BackendCompletion(#[source] CompletionError),

    /// The backend failed to record an exception.
    #[error("persisting exception: {0}")]
    BackendException(#[source] ExceptionError),
}

/// An [`waymark_vm_driver_core::EffectHandler`] that records workflow
/// completion outcomes to a backend.
///
/// Values are serialized via the given `Codec` before being persisted.
pub struct EffectHandler<Backend, Codec, ReadyValue>
where
    Backend: waymark_workflow_completion_backend::HasVmId,
{
    backend: Arc<Backend>,
    vm_id: Backend::VmId,
    codec: Codec,
    _phantom_data: PhantomData<ReadyValue>,
}

impl<Backend, Codec, ReadyValue> EffectHandler<Backend, Codec, ReadyValue>
where
    Backend: waymark_workflow_completion_backend::HasVmId,
{
    /// Create a new handler.
    pub fn new(backend: Arc<Backend>, vm_id: Backend::VmId, codec: Codec) -> Self {
        Self {
            backend,
            vm_id,
            codec,
            _phantom_data: PhantomData,
        }
    }
}

impl<Backend, Codec, ReadyValue> waymark_vm_driver_core::EffectHandler
    for EffectHandler<Backend, Codec, ReadyValue>
where
    Backend: waymark_workflow_completion_backend::RecordCompletion,
    Backend: waymark_workflow_completion_backend::RecordException,
    Backend: Send + Sync,
    Backend::VmId: Send,
    Codec: waymark_vm_codec_core::SerializerProvider + Send,
    ReadyValue: serde::Serialize + Send,
    waymark_vm_runtime_exception::Exception<ReadyValue>: serde::Serialize,
{
    type Effect = CoreSetEffect<ReadyValue>;
    type Error = HandleEffectError<
        Codec::Error,
        <Backend as waymark_workflow_completion_backend::RecordCompletion>::Error,
        <Backend as waymark_workflow_completion_backend::RecordException>::Error,
    >;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        match emitted_effect.effect {
            CoreSetEffect::Complete(value) => {
                tracing::info!("workflow completed successfully");
                let mut buf = Vec::new();
                self.codec
                    .with_serializer(&mut buf, |ser| serde::Serialize::serialize(&value, ser))
                    .map_err(HandleEffectError::Codec)?;
                self.backend
                    .record_completion(&self.vm_id, buf)
                    .await
                    .map_err(HandleEffectError::BackendCompletion)
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
                self.backend
                    .record_exception(&self.vm_id, buf)
                    .await
                    .map_err(HandleEffectError::BackendException)
            }
        }
    }
}
