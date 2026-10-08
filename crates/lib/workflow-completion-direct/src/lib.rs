//! Backend-free workflow completion handler that sends the outcome
//! directly through a [`tokio::sync::oneshot`] channel.
//!
//! This is an alternative to
//! `waymark_workflow_completion::EffectHandler` for transient /
//! in-memory execution where no durable persistence is needed and the
//! caller wants to await the result directly rather than polling a
//! backend.
//!
//! Unlike the persisting handler, the outcome is delivered as a typed value
//! rather than serialized bytes: producer and consumer live in the same
//! process, so there is nothing to serialize for, and the caller can convert
//! the value directly.

#![warn(missing_docs)]

use tokio::sync::oneshot;
use waymark_fullset_effect_handler::TerminalEffect;
use waymark_workflow_completion_core::Outcome;

/// An [`waymark_vm_driver_core::EffectHandler`] that sends the workflow
/// outcome directly through a [`tokio::sync::oneshot`] channel instead of
/// persisting to a backend.
///
/// This is useful for transient / in-memory execution where no durable
/// persistence is needed and the caller wants to await the result directly
/// rather than polling a backend.
pub struct DirectHandler<ReadyValue, RaisedException> {
    sender: Option<oneshot::Sender<Outcome<ReadyValue, RaisedException>>>,
}

impl<ReadyValue, RaisedException> DirectHandler<ReadyValue, RaisedException> {
    /// Create a new handler that will send the outcome through `sender`.
    pub fn new(sender: oneshot::Sender<Outcome<ReadyValue, RaisedException>>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
}

/// Error returned by [`DirectHandler`]'s [`handle_effect`](waymark_vm_driver_core::EffectHandler::handle_effect).
#[derive(Debug, thiserror::Error)]
pub enum DirectHandleEffectError {
    /// `handle_effect` was invoked again after the outcome was already sent.
    #[error("outcome was already sent")]
    AlreadyCompleted,
}

impl<ReadyValue, RaisedException> waymark_vm_driver_core::EffectHandler
    for DirectHandler<ReadyValue, RaisedException>
where
    ReadyValue: Send,
    RaisedException: Send + core::fmt::Debug,
{
    type Effect = TerminalEffect<ReadyValue, RaisedException>;
    type Error = DirectHandleEffectError;

    async fn handle_effect(
        &mut self,
        emitted_effect: waymark_vm_runtime_effect::EmittedEffect<Self::Effect>,
    ) -> Result<(), Self::Error> {
        let outcome = match emitted_effect.effect {
            TerminalEffect::Complete(value) => {
                tracing::debug!("workflow completed successfully");
                Outcome::Completion(value)
            }
            TerminalEffect::UnhandledException(exception) => {
                tracing::debug!(?exception, "workflow terminated with unhandled exception",);
                Outcome::Exception(exception)
            }
        };
        let sender = self
            .sender
            .take()
            .ok_or(DirectHandleEffectError::AlreadyCompleted)?;
        let _ = sender.send(outcome);
        Ok(())
    }
}
