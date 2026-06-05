//! Multi-VM runtime manager.
//!
//! This crate manages multiple [`waymark_vm_runtime::Runtime`] instances,
//! driving each one's execution loop, routing extcall effects and promise
//! settlements, and coordinating persistence via a backend.

#![warn(missing_docs)]

mod cleanup_guard;
mod error;

pub use self::error::*;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, debug, error};

use waymark_vm_codec_core::{SnapshotDeserializer, SnapshotSerializer};
use waymark_vm_driver_core::{EffectHandler, PromiseSettler, SnapshotPersister};
use waymark_vm_runtime::Runtime;
use waymark_vm_runtimes_manager_backend::{LoadSnapshot, StoreSnapshot};

/// Per-VM handles for lifecycle control.
struct VmHandle {
    /// Forcefully aborts the driver thread.
    abort: tokio::task::AbortHandle,
    /// Gracefully cancels the driver loop.
    cancel: CancellationToken,
}

/// Manages multiple VM runtimes and snapshot persistence.
pub struct VmRuntimesManager<VmId, Backend, Codec> {
    /// Snapshot storage backend, shared with driver threads.
    backend: Arc<Backend>,

    /// Snapshot codec.
    codec: Arc<Codec>,

    /// Active VM handles, keyed by VM id.
    /// Shared with cleanup tasks so they can remove entries on thread exit.
    handles: Arc<Mutex<HashMap<VmId, VmHandle>>>,
}

/// Adapter that binds a VM id to a shared backend, implementing
/// [`SnapshotPersister`] for the driver.
struct SnapshotAdapter<VmId, Backend> {
    vm_id: VmId,
    backend: Arc<Backend>,
}

impl<VmId, Backend> SnapshotPersister for SnapshotAdapter<VmId, Backend>
where
    Backend: StoreSnapshot<VmId = VmId> + Send + Sync,
    <Backend as StoreSnapshot>::Error: std::fmt::Debug,
    VmId: Sync,
{
    type Error = <Backend as StoreSnapshot>::Error;

    async fn persist_snapshot<'a>(&'a self, data: &'a [u8]) -> Result<(), Self::Error> {
        self.backend.store_snapshot(&self.vm_id, data.to_vec())
    }
}

impl<VmId, Backend, Codec> VmRuntimesManager<VmId, Backend, Codec> {
    /// Create a new manager.
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self {
            backend: Arc::new(backend),
            codec: Arc::new(codec),
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Gracefully shut down all VMs.
    ///
    /// Each driver exits at the next loop boundary;
    /// entries are cleaned up automatically when tasks finish.
    pub fn shutdown_all(&self) {
        for (_, vm) in self.handles.lock().unwrap().iter() {
            vm.cancel.cancel();
        }
    }

    /// Forcefully abort all VMs without waiting for graceful exit.
    pub fn force_shutdown_all(&self) {
        for (_, vm) in self.handles.lock().unwrap().drain() {
            vm.abort.abort();
        }
    }
}

impl<VmId, Backend, Codec> VmRuntimesManager<VmId, Backend, Codec>
where
    VmId: Eq + Hash,
{
    /// Gracefully shut down a specific VM.
    ///
    /// The driver will exit at the next loop boundary;
    /// the entry is cleaned up automatically when the task finishes.
    pub fn shutdown(&self, vm_id: &VmId) {
        if let Some(vm) = self.handles.lock().unwrap().get(vm_id) {
            vm.cancel.cancel();
        }
    }

    /// Forcefully abort a specific VM without waiting for graceful exit.
    pub fn force_shutdown(&self, vm_id: &VmId) {
        if let Some(vm) = self.handles.lock().unwrap().remove(vm_id) {
            vm.abort.abort();
        }
    }
}

impl<VmId, Backend, Codec> VmRuntimesManager<VmId, Backend, Codec>
where
    VmId: Eq + Hash + Clone + Send + core::fmt::Debug + 'static + Sync,
    Backend: StoreSnapshot<VmId = VmId> + Send + Sync + 'static,
    <Backend as StoreSnapshot>::Error: std::fmt::Debug,
    Codec: SnapshotSerializer + Send + Sync + 'static,
{
    /// Spawn a new VM runtime on a dedicated OS thread.
    ///
    /// Returns an error if a VM with the same id is already running.
    pub async fn spawn<Executable, Interpreter, Value, EffectsHandler>(
        &self,
        vm_id: VmId,
        runtime: Runtime<Executable, Interpreter, Value>,
        effects_handler: EffectsHandler,
    ) -> Result<(), SpawnError<VmId>>
    where
        EffectsHandler: EffectHandler<Effect = Interpreter::Effect>
            + PromiseSettler<Value = Value::ReadyValue, Ack: Send>
            + Send
            + 'static,
        Executable: waymark_vm_executable::InstructionsProvider + Send + 'static,
        Executable::FunctionId: Copy + Serialize + Send,
        Executable::StateId: Copy + PartialEq + Serialize + Send,
        Executable: waymark_vm_executable::FunctionStates + Send,
        Interpreter: waymark_vm_interpreter::Interpreter<
                Frame = waymark_vm_runtime::FrameFor<Executable, Value>,
                Instruction = Executable::Instruction,
            > + Send
            + 'static,
        Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
                Executable,
                Executable::FunctionId,
                Executable::StateId,
                Value,
                RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<
                    'r,
                >,
            >,
        Value: Clone + Send + 'static + Serialize,
        Value: waymark_vm_runtime_promise_core::Resolvable,
        Interpreter::Error: core::fmt::Debug + Send,
        Interpreter::Effect: core::fmt::Debug + Send,
        Interpreter::Instruction: core::fmt::Debug,
        Value: core::fmt::Debug,
        Value::ReadyValue: core::fmt::Debug + Send,
        <Backend as StoreSnapshot>::Error: Send,
        <Codec as SnapshotSerializer>::Error: Send,
        <EffectsHandler as EffectHandler>::Error: Send,
        <EffectsHandler as PromiseSettler>::Error: Send,
    {
        {
            let handles = self.handles.lock().unwrap();
            if handles.contains_key(&vm_id) {
                return Err(SpawnError::AlreadyRunning(vm_id));
            }
        }

        let backend = Arc::clone(&self.backend);
        let codec = Arc::clone(&self.codec);
        let snapshotter = SnapshotAdapter {
            vm_id: vm_id.clone(),
            backend,
        };

        {
            let span = tracing::info_span!("drive_runtime", ?vm_id);
            let cancel = CancellationToken::new();
            let driver = waymark_vm_driver_thread::spawn(waymark_vm_driver::Params {
                runtime,
                effector: effects_handler,
                persister: snapshotter,
                codec,
                cancel: cancel.clone(),
            });

            let abort_handle = driver.abort_handle();
            self.handles.lock().unwrap().insert(
                vm_id.clone(),
                VmHandle {
                    abort: abort_handle,
                    cancel,
                },
            );

            tokio::spawn({
                let _guard =
                    cleanup_guard::CleanupGuard::new(Arc::clone(&self.handles), vm_id.clone());
                async move {
                    let Err(error) = driver.await;
                    match error {
                        waymark_vm_driver_thread::Error::Driver(error) => {
                            error!(?error, "vm driver terminated");
                        }
                        waymark_vm_driver_thread::Error::Thread(error) => {
                            error!(?error, "vm thread panicked");
                        }
                    }
                    debug!("driver thread exited");
                }
                .instrument(span)
            });
        }

        Ok(())
    }
}

impl<VmId, Backend, Codec> VmRuntimesManager<VmId, Backend, Codec>
where
    VmId: Eq + Hash + Clone + Send + core::fmt::Debug + 'static + Sync,
    Backend: StoreSnapshot<VmId = VmId> + LoadSnapshot<VmId = VmId> + Send + Sync + 'static,
    <Backend as StoreSnapshot>::Error: std::fmt::Debug,
    <Backend as LoadSnapshot>::Error: std::fmt::Debug,
    Codec: SnapshotSerializer + SnapshotDeserializer + Send + Sync + 'static,
    <Codec as SnapshotDeserializer>::Error: std::error::Error + 'static,
{
    /// Revive a VM from a persisted snapshot.
    pub async fn revive<Executable, Interpreter, Value, EffectsHandler>(
        &self,
        vm_id: VmId,
        interpreter: Interpreter,
        executable: Executable,
        effects_handler: EffectsHandler,
    ) -> Result<
        (),
        ReviveError<VmId, <Backend as LoadSnapshot>::Error, <Codec as SnapshotDeserializer>::Error>,
    >
    where
        EffectsHandler: EffectHandler<Effect = Interpreter::Effect>
            + PromiseSettler<Value = Value::ReadyValue, Ack: Send>
            + Send
            + 'static,
        Executable: waymark_vm_executable::InstructionsProvider + Send + 'static,
        Executable::FunctionId: Copy + for<'de> serde::Deserialize<'de> + Serialize + Send,
        Executable::StateId: Copy + PartialEq + for<'de> serde::Deserialize<'de> + Serialize + Send,
        Executable: waymark_vm_executable::FunctionStates + Send,
        Interpreter: waymark_vm_interpreter::Interpreter<
                Frame = waymark_vm_runtime::FrameFor<Executable, Value>,
                Instruction = Executable::Instruction,
            > + Send
            + 'static,
        Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
                Executable,
                Executable::FunctionId,
                Executable::StateId,
                Value,
                RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<
                    'r,
                >,
            >,
        Value: Clone + Send + 'static + for<'de> serde::Deserialize<'de> + Serialize,
        Value: waymark_vm_runtime_promise_core::Resolvable,
        Interpreter::Error: core::fmt::Debug + Send,
        Interpreter::Effect: core::fmt::Debug + Send,
        Interpreter::Instruction: core::fmt::Debug,
        Value: core::fmt::Debug,
        Value::ReadyValue: core::fmt::Debug + Send,
        <Backend as StoreSnapshot>::Error: Send,
        <Codec as SnapshotSerializer>::Error: Send,
        <EffectsHandler as EffectHandler>::Error: Send,
        <EffectsHandler as PromiseSettler>::Error: Send,
    {
        let data = self
            .backend
            .load_snapshot(&vm_id)
            .await
            .map_err(ReviveError::Load)?;

        let runtime = self
            .codec
            .with_deserializer(&data, |de| {
                Runtime::from_snapshot(interpreter, executable, de)
            })
            .map_err(|error| ReviveError::DeserializationFailed {
                vm_id: vm_id.clone(),
                error,
            })?;

        self.spawn(vm_id, runtime, effects_handler)
            .await
            .map_err(ReviveError::Spawn)
    }
}
