//! Multi-VM runtime manager, backed by [`waymark_state_manager::State`].
//!
//! This crate manages multiple [`waymark_vm_runtime::Runtime`] instances,
//! driving each one's execution loop, routing extcall effects and promise
//! settlements, and coordinating persistence via a backend.
//!
//! The state-manager provides concurrent access and automatic cleanup
//! through ref-counted [`Handle`](waymark_state_manager::Handle)s. When
//! the last handle to a VM is dropped, the entry is marked for eviction
//! and eventually swept (which drops the [`Spawned`], aborting the
//! underlying driver thread).

#![warn(missing_docs)]

mod once_receiver;
mod snapshot_adapter;
mod spawner;

pub use self::snapshot_adapter::SnapshotAdapter;
pub use self::spawner::{ErrorFor, Evicted, Spawned};

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use tracing::Instrument as _;

/// Error from [`SpawningFactory::produce`].
#[derive(Debug, thiserror::Error)]
pub enum SpawningError<LoadError, ExecutableProviderError, DeserializeError>
where
    DeserializeError: std::error::Error + 'static,
{
    /// Failed to load the persisted snapshot.
    #[error("load: {0}")]
    Load(#[source] LoadError),

    /// Failed to load the bytecode executable.
    #[error("executable provider: {0}")]
    ExecutableProvider(#[source] ExecutableProviderError),

    /// Failed to deserialize the snapshot.
    #[error("deserialization failed: {0}")]
    Deserialization(#[source] DeserializeError),
}

/// A [`waymark_state_manager_core::Factory`] that revives VMs from persisted
/// snapshots.
pub struct SpawningFactory<
    Backend,
    Codec,
    ExecutableProvider,
    InterpreterProvider,
    EffectorProvider,
    Value,
> {
    backend: Arc<Backend>,
    codec: Arc<Codec>,
    executable_provider: ExecutableProvider,
    interpreter_provider: InterpreterProvider,
    effector_provider: EffectorProvider,
    _phantom_data: PhantomData<Value>,
}

impl<Backend, Codec, ExecutableProvider, InterpreterProvider, EffectorProvider, Value>
    SpawningFactory<
        Backend,
        Codec,
        ExecutableProvider,
        InterpreterProvider,
        EffectorProvider,
        Value,
    >
{
    /// Create a new spawning factory.
    pub fn new(
        backend: Arc<Backend>,
        codec: Arc<Codec>,
        executable_provider: ExecutableProvider,
        interpreter_provider: InterpreterProvider,
        effector_provider: EffectorProvider,
    ) -> Self {
        Self {
            backend,
            codec,
            executable_provider,
            interpreter_provider,
            effector_provider,
            _phantom_data: PhantomData,
        }
    }
}

impl<Backend, Codec, ExecutableProvider, InterpreterProvider, EffectorProvider, Value>
    waymark_state_manager_core::Factory
    for SpawningFactory<
        Backend,
        Codec,
        ExecutableProvider,
        InterpreterProvider,
        EffectorProvider,
        Value,
    >
where
    Backend: waymark_state_vm_runtimes_backend::HasVmId,
    Backend::VmId: Hash + Eq + Clone + Send + Sync + core::fmt::Debug + 'static,
    Backend: waymark_state_vm_runtimes_backend::StoreSnapshots,
    Backend: waymark_state_vm_runtimes_backend::LoadForRevive,
    Backend: Send + Sync + 'static,
    <Backend as waymark_state_vm_runtimes_backend::StoreSnapshots>::Error:
        std::fmt::Debug + Send + 'static,
    <Backend as waymark_state_vm_runtimes_backend::LoadForRevive>::Error:
        std::fmt::Debug + Send + 'static,
    Codec: waymark_vm_codec_core::SerializerProvider<Ok = ()>,
    Codec: waymark_vm_codec_core::DeserializerProvider,
    Codec: Send + Sync + 'static,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error: Send + 'static,
    <Codec as waymark_vm_codec_core::DeserializerProvider>::Error:
        std::error::Error + Send + 'static,
    Backend: waymark_state_vm_runtimes_backend::HasExecutableId,
    <Backend as waymark_state_vm_runtimes_backend::HasExecutableId>::ExecutableId:
        Eq + Hash + Clone + Send + Sync,
    ExecutableProvider: waymark_state_manager::provider::Provider<
        Key = <Backend as waymark_state_vm_runtimes_backend::HasExecutableId>::ExecutableId,
    >,
    ExecutableProvider: Send + Sync + 'static,
    ExecutableProvider::Error: Send + 'static,
    ExecutableProvider::Value:
        waymark_vm_executable::InstructionsProvider + Send + Sync + 'static + Clone,
    <ExecutableProvider::Value as waymark_vm_executable::Functions>::FunctionId: Copy + Send,
    <ExecutableProvider::Value as waymark_vm_executable::Functions>::FunctionId:
        serde::Serialize,
    <ExecutableProvider::Value as waymark_vm_executable::Functions>::FunctionId:
        for<'a> serde::Deserialize<'a>,
    <ExecutableProvider::Value as waymark_vm_executable::FunctionStates>::StateId:
        Copy + PartialEq + Send,
    <ExecutableProvider::Value as waymark_vm_executable::FunctionStates>::StateId:
        serde::Serialize,
    <ExecutableProvider::Value as waymark_vm_executable::FunctionStates>::StateId:
        for<'a> serde::Deserialize<'a>,
    ExecutableProvider::Value: waymark_vm_executable::FunctionStates + Send,
    InterpreterProvider: waymark_state_vm_runtimes_core::InterpreterProvider<
        VmId = Backend::VmId,
    >,
    InterpreterProvider: Send + Sync + 'static,
    InterpreterProvider::Interpreter: waymark_vm_interpreter::Interpreter<
            Frame = waymark_vm_runtime::FrameFor<ExecutableProvider::Value, Value>,
            Instruction = <ExecutableProvider::Value as waymark_vm_executable::InstructionsProvider>::Instruction,
        >,
    InterpreterProvider::Interpreter: Send + 'static,
    InterpreterProvider::Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
            ExecutableProvider::Value,
            <ExecutableProvider::Value as waymark_vm_executable::Functions>::FunctionId,
            <ExecutableProvider::Value as waymark_vm_executable::FunctionStates>::StateId,
            Value,
            RuntimeView<'r> = <InterpreterProvider::Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'r>,
        >,
    EffectorProvider: waymark_state_vm_runtimes_core::EffectorProvider<
        VmId = Backend::VmId,
    >,
    EffectorProvider: Send + Sync + 'static,
    EffectorProvider::Effector: waymark_vm_driver_core::EffectHandler<
            Effect = <InterpreterProvider::Interpreter as waymark_vm_interpreter::Interpreter>::Effect,
        > + waymark_vm_driver_core::PromiseSettler<Value = Value::ReadyValue, Ack: Send>
        + Send
        + Sync
        + 'static,
    Value: Clone + Send + Sync + 'static,
    Value: serde::Serialize + for<'a> serde::Deserialize<'a>,
    Value: waymark_vm_runtime_promise_core::Resolvable + core::fmt::Debug,
    Value::ReadyValue: core::fmt::Debug + Send,
    <InterpreterProvider::Interpreter as waymark_vm_interpreter::Interpreter>::Error:
        core::fmt::Debug + Send,
    <InterpreterProvider::Interpreter as waymark_vm_interpreter::Interpreter>::Effect:
        core::fmt::Debug + Send,
    <InterpreterProvider::Interpreter as waymark_vm_interpreter::Interpreter>::Instruction:
        core::fmt::Debug,
    <EffectorProvider::Effector as waymark_vm_driver_core::EffectHandler>::Error:
        Send + 'static,
    <EffectorProvider::Effector as waymark_vm_driver_core::PromiseSettler>::Error:
        Send + 'static,
{
    type Key = Backend::VmId;
    type Value = Arc<
        Spawned<
            ErrorFor<
                InterpreterProvider::Interpreter,
                Codec,
                SnapshotAdapter<Backend::VmId, Backend>,
                EffectorProvider::Effector,
            >,
        >,
    >;
    type Error = SpawningError<
        <Backend as waymark_state_vm_runtimes_backend::LoadForRevive>::Error,
        ExecutableProvider::Error,
        <Codec as waymark_vm_codec_core::DeserializerProvider>::Error,
    >;

    #[tracing::instrument(skip_all, fields(vm_id = ?key))]
    async fn produce(&self, key: &Self::Key) -> Result<Self::Value, Self::Error> {
        let revive_payload = self
            .backend
            .load_for_revive(key)
            .await
            .map_err(SpawningError::Load)?;


        let waymark_state_vm_runtimes_backend::RevivePayload { snapshot, executable_id } = revive_payload;

        let executable_handle = self
            .executable_provider
            .get(executable_id)
            .await
            .map_err(SpawningError::ExecutableProvider)?;

        let executable = (*executable_handle).clone();

        let interpreter = self.interpreter_provider.provide_interpreter(key);

        let runtime = self.codec.with_deserializer(&snapshot, |de| {
            waymark_vm_runtime::Runtime::from_snapshot(
                interpreter,
                executable,
                de,
            )
        }).map_err(SpawningError::Deserialization)?;


        let snapshotter = snapshot_adapter::SnapshotAdapter {
            vm_id: key.clone(),
            backend: Arc::clone(&self.backend),
        };

        let spawned = spawner::spawn(
            Arc::clone(&self.codec),
            runtime,
            self.effector_provider.provide_effector(key),
            snapshotter,
            vec![Box::new(executable_handle)],
        )
        .instrument(tracing::info_span!("drive_runtime", ?key))
        .await;

        Ok(Arc::new(spawned))
    }
}
