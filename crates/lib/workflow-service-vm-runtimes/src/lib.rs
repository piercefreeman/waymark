//! Workflow service — encapsulates creating workflows and polling for
//! their outcomes.

#![warn(missing_docs)]

use std::{marker::PhantomData, time::Duration};

use waymark_vm_codec_core::{DeserializerProvider, SerializerProvider};

use waymark_workflow_completion_core::Outcome;

/// Errors returned by [`RegistrationService::register_vm`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterVmError<RegistrationError, CodecError> {
    /// The runtime snapshot serialization failed.
    #[error("serialize runtime snapshot: {0:?}")]
    Serialize(CodecError),

    /// The VM runtime registration failed.
    #[error("register vm runtime: {0:?}")]
    Registration(RegistrationError),

    /// A VM runtime is already registered under this id; it was left
    /// untouched.
    #[error("vm runtime already registered")]
    AlreadyRegistered,
}

/// Errors returned by [`RegistrationService::register_vms`].
///
/// Already-registered ids are not errors — they are reported via
/// [`RegistrationSuccess::SomeAlreadyRegistered`](waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::SomeAlreadyRegistered).
#[derive(Debug, thiserror::Error)]
pub enum RegisterVmsError<RegistrationError, CodecError> {
    /// A runtime snapshot serialization failed.
    #[error("serialize runtime snapshot: {0:?}")]
    Serialize(CodecError),

    /// The VM runtime registration failed.
    #[error("register vm runtimes: {0:?}")]
    Registration(RegistrationError),
}

/// Errors returned by [`OutcomePollingService::wait_for_outcome`].
#[derive(Debug, thiserror::Error)]
pub enum WaitForOutcomeError<PollError, ExistsError, CodecError> {
    /// The outcome poll failed.
    #[error("poll outcome: {0:?}")]
    Poll(PollError),

    /// The registered-runtime existence check failed.
    #[error("find existing vm runtime: {0:?}")]
    Exists(ExistsError),

    /// No VM runtime was ever registered for the requested id.
    #[error("no vm runtime registered for the requested id")]
    NotFound,

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
    Backend: waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntimes,
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
            <Backend as waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntimes>::Error,
            <Codec as SerializerProvider>::Error,
        >,
    > {
        let mut snapshot = Vec::new();
        self.codec
            .with_serializer(&mut snapshot, snapshot_provider)
            .map_err(RegisterVmError::Serialize)?;
        let item =
            waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegisterVmRuntimesItem {
                vm_id: &vm_id,
                executable_id: &executable_id,
                snapshot: &snapshot,
            };
        let success = self
            .backend
            .register_vm_runtimes(nonempty_collections::nev![item].as_nonempty_slice())
            .await
            .map_err(RegisterVmError::Registration)?;

        match success {
            waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::AllRegistered => Ok(()),
            // The batch held exactly this VM, so a conflict can only name it.
            waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess::SomeAlreadyRegistered(_) => {
                Err(RegisterVmError::AlreadyRegistered)
            }
        }
    }

    /// Register a batch of VM runtimes, serializing each into a snapshot,
    /// with one backend call for the whole batch.
    ///
    /// Each entry carries its per-runtime `SnapshotData`; one
    /// `snapshot_provider` serializes them all — a collection of
    /// same-typed closures would be equivalent to exactly this, with the
    /// captured state made explicit.
    ///
    /// Returns the per-batch
    /// [`RegistrationSuccess`](waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess)
    /// verbatim — already-registered ids are per-row facts, not errors.
    pub async fn register_vms<SnapshotData, SnapshotProvider>(
        &self,
        vms: nonempty_collections::NEVec<(Backend::VmId, Backend::ExecutableId, SnapshotData)>,
        mut snapshot_provider: SnapshotProvider,
    ) -> Result<
        waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegistrationSuccess<
            Backend::VmId,
        >,
        RegisterVmsError<
            <Backend as waymark_workflow_service_vm_runtimes_backend::RegisterVmRuntimes>::Error,
            <Codec as SerializerProvider>::Error,
        >,
    >
    where
        SnapshotProvider: for<'buf> FnMut(
            SnapshotData,
            Codec::Serializer<'buf>,
        )
            -> Result<(), <Codec as SerializerProvider>::Error>,
    {
        let mut serialized = Vec::with_capacity(vms.len().get());
        for (vm_id, executable_id, snapshot_data) in vms {
            let mut snapshot = Vec::new();
            self.codec
                .with_serializer(&mut snapshot, |serializer| {
                    snapshot_provider(snapshot_data, serializer)
                })
                .map_err(RegisterVmsError::Serialize)?;
            serialized.push((vm_id, executable_id, snapshot));
        }

        let items: Vec<_> = serialized
            .iter()
            .map(|(vm_id, executable_id, snapshot)| {
                waymark_workflow_service_vm_runtimes_backend::register_vm_runtimes::RegisterVmRuntimesItem {
                    vm_id,
                    executable_id,
                    snapshot: snapshot.as_slice(),
                }
            })
            .collect();
        self.backend
            .register_vm_runtimes(
                nonempty_collections::NESlice::try_from_slice(&items)
                    .expect("built from a non-empty input"),
            )
            .await
            .map_err(RegisterVmsError::Registration)
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
    Backend: waymark_workflow_service_vm_runtimes_backend::FindExistingVmRuntimes<
            VmId = <Backend as waymark_workflow_completion_backend::HasVmId>::VmId,
        >,
    Codec: DeserializerProvider,
    Value: for<'de> serde::Deserialize<'de>,
{
    /// Poll for the outcome of a workflow instance.
    ///
    /// Blocks until an outcome is recorded or the caller cancels. Returns
    /// [`WaitForOutcomeError::NotFound`] if no VM runtime was ever registered
    /// for `vm_id` — the existence check runs at most once, only when the first
    /// poll finds no outcome, so the steady-state poll loop stays a
    /// single-table query.
    pub async fn wait_for_outcome(
        &self,
        vm_id: &<Backend as waymark_workflow_completion_backend::HasVmId>::VmId,
        poll_interval: Duration,
    ) -> Result<
        Outcome<Value>,
        WaitForOutcomeError<
            <Backend as waymark_workflow_completion_backend::PollOutcome>::Error,
            <Backend as waymark_workflow_service_vm_runtimes_backend::FindExistingVmRuntimes>::Error,
            <Codec as DeserializerProvider>::Error,
        >,
    >{
        let mut checked = false;
        loop {
            match self.backend.poll_outcome(vm_id).await {
                Ok(Some(raw)) => {
                    return decode(&self.codec, raw).map_err(WaitForOutcomeError::Deserialize);
                }
                Ok(None) => {
                    if !checked {
                        let vm_ids = nonempty_collections::NESlice::try_from_slice(
                            std::slice::from_ref(vm_id),
                        )
                        .expect("from_ref yields a one-element, non-empty slice");
                        let existing = self
                            .backend
                            .find_existing_vm_runtimes(vm_ids)
                            .await
                            .map_err(WaitForOutcomeError::Exists)?;
                        if existing.is_empty() {
                            return Err(WaitForOutcomeError::NotFound);
                        }
                        checked = true;
                    }
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
