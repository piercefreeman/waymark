//! Backend-free workflow completion handler that sends the outcome
//! directly through a [`tokio::sync::oneshot`] channel.
//!
//! This is an alternative to
//! [`waymark_workflow_completion::EffectHandler`] for transient /
//! in-memory execution where no durable persistence is needed and the
//! caller wants to await the result directly rather than polling a
//! backend.

#![warn(missing_docs)]

use std::marker::PhantomData;

use tokio::sync::oneshot;
use waymark_vm_interpreter_coreset::Effect as CoreSetEffect;

pub use waymark_workflow_completion_backend::Outcome;

/// An [`waymark_vm_driver_core::EffectHandler`] that sends the workflow
/// outcome directly through a [`tokio::sync::oneshot`] channel instead of
/// persisting to a backend.
///
/// This is useful for transient / in-memory execution where no durable
/// persistence is needed and the caller wants to await the result directly
/// rather than polling a backend.
pub struct DirectHandler<Codec, ReadyValue> {
    sender: Option<oneshot::Sender<Outcome>>,
    codec: Codec,
    _phantom_data: PhantomData<ReadyValue>,
}

impl<Codec, ReadyValue> DirectHandler<Codec, ReadyValue> {
    /// Create a new handler that will send the outcome through `sender`.
    pub fn new(sender: oneshot::Sender<Outcome>, codec: Codec) -> Self {
        Self {
            sender: Some(sender),
            codec,
            _phantom_data: PhantomData,
        }
    }
}

/// Error returned by [`DirectHandler::handle_effect`].
#[derive(Debug, thiserror::Error)]
pub enum DirectHandleEffectError<CodecError> {
    /// Serialization of the completion value or exception failed.
    #[error("codec: {0:?}")]
    Codec(#[source] CodecError),

    /// The receiver was dropped before the outcome could be sent.
    #[error("receiver dropped")]
    ReceiverDropped,
}

impl<Codec, ReadyValue> waymark_vm_driver_core::EffectHandler for DirectHandler<Codec, ReadyValue>
where
    Codec: waymark_vm_codec_core::SerializerProvider + Send,
    ReadyValue: serde::Serialize + Send,
    waymark_vm_runtime_exception::Exception<ReadyValue>: serde::Serialize,
{
    type Effect = CoreSetEffect<ReadyValue>;
    type Error = DirectHandleEffectError<Codec::Error>;

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
                    .map_err(DirectHandleEffectError::Codec)?;
                Outcome::Completion(buf)
            }
            CoreSetEffect::UnhandledException(exception) => {
                tracing::info!(
                    exception_type = %exception.type_id,
                    "workflow terminated with unhandled exception",
                );
                let mut buf = Vec::new();
                self.codec
                    .with_serializer(&mut buf, |ser| serde::Serialize::serialize(&exception, ser))
                    .map_err(DirectHandleEffectError::Codec)?;
                Outcome::Exception(buf)
            }
        };
        let sender = self
            .sender
            .take()
            .ok_or(DirectHandleEffectError::ReceiverDropped)?;
        let _ = sender.send(outcome);
        Ok(())
    }
}
