//! Workflow service — encapsulates creating workflows and polling for
//! their outcomes.

#![warn(missing_docs)]

use std::{marker::PhantomData, time::Duration};

use waymark_vm_codec_core::{DeserializerProvider, SerializerProvider};

/// A decoded workflow outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome<Value> {
    /// The workflow completed successfully.
    Completion(Value),

    /// The workflow terminated with an unhandled exception.
    Exception(waymark_vm_runtime_exception::Exception<Value>),
}

/// Errors returned by [`RegistrationService::register_vm`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterVmError<RegistrationError, CodecError> {
    /// The runtime snapshot serialization failed.
    #[error("serialize runtime snapshot: {0:?}")]
    Serialize(CodecError),

    /// The VM runtime registration failed.
    #[error("register vm runtime: {0:?}")]
    Registration(RegistrationError),
}

/// Errors returned by [`OutcomePollingService::wait_for_outcome`].
#[derive(Debug, thiserror::Error)]
pub enum WaitForOutcomeError<PollError, CodecError> {
    /// The outcome poll failed.
    #[error("poll outcome: {0:?}")]
    Poll(PollError),

    /// The outcome deserialization failed.
    #[error("deserialize outcome: {0:?}")]
    Deserialize(CodecError),
}

/// High-level service for registering workflow VMs.
pub struct RegistrationService<Backend, Codec> {
    backend: Backend,
    codec: Codec,
}

impl<Backend, Codec> RegistrationService<Backend, Codec> {
    /// Create a new workflow service wrapping the given backend and codec.
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self { backend, codec }
    }
}

impl<Backend, Codec> RegistrationService<Backend, Codec>
where
    Backend: waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime,
    Codec: SerializerProvider,
{
    /// Register a VM runtime by serializing it into a snapshot.
    pub async fn register_vm(
        &self,
        vm_id: Backend::VmId,
        executable_id: Backend::ExecutableId,
        snapshot_provider: impl for<'buf> FnOnce(
            Codec::Serializer<'buf>,
        )
            -> Result<(), <Codec as SerializerProvider>::Error>,
    ) -> Result<
        (),
        RegisterVmError<
            <Backend as waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntime>::Error,
            <Codec as SerializerProvider>::Error,
        >,
    > {
        let mut snapshot = Vec::new();
        self.codec
            .with_serializer(&mut snapshot, snapshot_provider)
            .map_err(RegisterVmError::Serialize)?;
        self.backend
            .register_vm_runtime(&vm_id, &executable_id, &snapshot)
            .await
            .map_err(RegisterVmError::Registration)
    }
}

/// High-level service for polling workflow outcomes.
pub struct OutcomePollingService<Backend, Codec, Value> {
    backend: Backend,
    codec: Codec,
    phantom_data: PhantomData<Value>,
}

impl<Backend, Codec, Value> OutcomePollingService<Backend, Codec, Value> {
    /// Create a new outcome polling service wrapping the given backend and
    /// codec.
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self {
            backend,
            codec,
            phantom_data: PhantomData,
        }
    }
}

impl<Backend, Codec, Value> OutcomePollingService<Backend, Codec, Value>
where
    Backend: waymark_workflow_completion_backend::PollOutcome,
    Codec: DeserializerProvider,
    Value: for<'de> serde::Deserialize<'de>,
{
    /// Poll for the outcome of a workflow instance.
    ///
    /// Blocks until an outcome is recorded or the caller cancels.
    pub async fn wait_for_outcome(
        &self,
        vm_id: &Backend::VmId,
        poll_interval: Duration,
    ) -> Result<
        Outcome<Value>,
        WaitForOutcomeError<
            <Backend as waymark_workflow_completion_backend::PollOutcome>::Error,
            <Codec as DeserializerProvider>::Error,
        >,
    > {
        loop {
            match self.backend.poll_outcome(vm_id).await {
                Ok(Some(raw)) => {
                    return decode(&self.codec, raw).map_err(WaitForOutcomeError::Deserialize);
                }
                Ok(None) => {
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
                Err(err) => {
                    return Err(WaitForOutcomeError::Poll(err));
                }
            }
        }
    }
}

fn decode<Codec, Value>(
    codec: &Codec,
    raw: waymark_workflow_completion_backend::Outcome,
) -> Result<Outcome<Value>, Codec::Error>
where
    Codec: DeserializerProvider,
    Value: for<'de> serde::Deserialize<'de>,
{
    match raw {
        waymark_workflow_completion_backend::Outcome::Completion(bytes) => {
            let value =
                codec.with_deserializer(&bytes, |de| serde::Deserialize::deserialize(de))?;
            Ok(Outcome::Completion(value))
        }
        waymark_workflow_completion_backend::Outcome::Exception(bytes) => {
            let exception =
                codec.with_deserializer(&bytes, |de| serde::Deserialize::deserialize(de))?;
            Ok(Outcome::Exception(exception))
        }
    }
}
