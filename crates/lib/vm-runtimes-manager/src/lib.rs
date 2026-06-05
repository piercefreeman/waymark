//! Multi-VM runtime manager.
//!
//! This crate manages multiple [`Runtime`](waymark_vm_runtime::Runtime) instances,
//! driving each one's execution loop, routing extcall effects and promise
//! settlements, and coordinating persistence via a backend.
//!
//! # Architecture
//!
//! Each VM is driven by a dedicated OS thread that loops:
//!
//! ```text
//! runtime.run() → effect → effects_tx  → update VmState, check policy
//!              → NoReadyFrame → await settlement → resolve → runtime.run() …
//! ```
//!
//! Lifecycle decisions are made **on the driver thread**, colocated with the
//! runtime. When a policy recommends persist or evict, the driver serializes
//! the runtime and sends the bytes to the manager for storage.

#![warn(missing_docs)]

mod error;

pub use self::error::*;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use waymark_vm_driver::{Params, run};
use waymark_vm_driver_core::{EffectHandler, PromiseSettler, SnapshotPersister};
use waymark_vm_lifecycle::LifecyclePolicy;
use waymark_vm_managed_runtime::ManagedRuntime;
use waymark_vm_runtime::Runtime;
use waymark_vm_runtimes_manager_backend::{LoadSnapshot, StoreSnapshot};

/// Manages multiple VM runtimes and snapshot persistence.
///
/// Generic over:
/// - `VmId`: unique VM identifier.
/// - `Backend`: implements both [`SnapshotStore`] and [`LoadSnapshot`].
/// - `Policy`: lifecycle policy ([`LifecyclePolicy`]), shared by all VMs.
pub struct VmRuntimesManager<VmId, Backend, Policy> {
    /// Snapshot storage backend, shared with driver threads.
    backend: Arc<Backend>,

    /// Lifecycle policy, cloned for each driver thread.
    #[allow(dead_code)]
    policy: Policy,

    /// Active driver thread handles, keyed by VM id.
    /// Shared with cleanup tasks so they can remove entries on thread exit.
    handles: Arc<Mutex<HashMap<VmId, CancellationToken>>>,
}

/// A spawned driver thread.
///
/// Hold onto this to cancel the VM later via the [`CancellationToken`].
#[must_use]
pub struct Spawned {
    /// Cancel token for the spawned driver thread.
    pub cancel: CancellationToken,
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

impl<VmId, Backend, Policy> VmRuntimesManager<VmId, Backend, Policy> {
    /// Create a new manager.
    pub fn new(backend: Backend, policy: Policy) -> Self {
        Self {
            backend: Arc::new(backend),
            policy,
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Shut down all VMs without snapshotting.
    pub fn shutdown_all(&self) {
        for (_, cancel) in self.handles.lock().unwrap().drain() {
            cancel.cancel();
        }
    }
}

impl<VmId, Backend, Policy> VmRuntimesManager<VmId, Backend, Policy>
where
    VmId: Eq + Hash,
{
    /// Shut down a specific VM without snapshotting.
    pub fn shutdown(&self, vm_id: &VmId) {
        if let Some(cancel) = self.handles.lock().unwrap().remove(vm_id) {
            cancel.cancel();
        }
    }
}

impl<VmId, Backend, Policy> VmRuntimesManager<VmId, Backend, Policy>
where
    VmId: Eq + Hash + Clone + Send + core::fmt::Debug + 'static + Sync,
    Backend: StoreSnapshot<VmId = VmId> + Send + Sync + 'static,
    <Backend as StoreSnapshot>::Error: std::fmt::Debug,
    Policy: LifecyclePolicy + Clone + Send + 'static,
{
    /// Spawn a new VM runtime on a dedicated OS thread.
    ///
    /// Returns an error if a VM with the same id is already running.
    pub async fn spawn<Executable, Interpreter, Value, EffectsHandler>(
        &mut self,
        vm_id: VmId,
        runtime: ManagedRuntime<Executable, Interpreter, Value>,
        effects_handler: EffectsHandler,
    ) -> Result<Spawned, SpawnError<VmId>>
    where
        EffectsHandler: EffectHandler<Effect = Interpreter::Effect>
            + PromiseSettler<Value = Value::ReadyValue>
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
        Value::ReadyValue: core::fmt::Debug,
    {
        {
            let handles = self.handles.lock().unwrap();
            if handles.contains_key(&vm_id) {
                return Err(SpawnError::AlreadyRunning(vm_id));
            }
        }

        let backend = Arc::clone(&self.backend);
        let cancel = CancellationToken::new();
        let snapshotter = SnapshotAdapter {
            vm_id: vm_id.clone(),
            backend,
        };

        {
            let handles = Arc::clone(&self.handles);
            let vm_id_for_cleanup = vm_id.clone();
            let task = {
                let span = tracing::info_span!("drive_runtime", ?vm_id);
                let handle = tokio::runtime::Handle::current();
                tokio::task::spawn_blocking(move || {
                    handle.block_on(async {
                        let _guard = span.enter();
                        let _ = run(Params {
                            runtime: runtime.into(),
                            effector: effects_handler,
                            persister: snapshotter,
                        })
                        .await;
                    });
                })
            };
            tokio::spawn(async move {
                let _ = task.await;
                handles.lock().unwrap().remove(&vm_id_for_cleanup);
                debug!(vm_id = ?vm_id_for_cleanup, "driver thread exited");
            });
        }

        self.handles
            .lock()
            .unwrap()
            .insert(vm_id.clone(), cancel.clone());
        Ok(Spawned { cancel })
    }
}

impl<VmId, Backend, Policy> VmRuntimesManager<VmId, Backend, Policy>
where
    VmId: Eq + Hash + Clone + Send + core::fmt::Debug + 'static + Sync,
    Backend: StoreSnapshot<VmId = VmId> + LoadSnapshot<VmId = VmId> + Send + Sync + 'static,
    <Backend as StoreSnapshot>::Error: std::fmt::Debug,
    <Backend as LoadSnapshot>::Error: std::fmt::Debug,
    Policy: LifecyclePolicy + Clone + Send + 'static,
{
    /// Revive a VM from a persisted snapshot.
    pub async fn revive<Executable, Interpreter, Value, EffectsHandler>(
        &mut self,
        vm_id: VmId,
        interpreter: Interpreter,
        executable: Executable,
        effects_handler: EffectsHandler,
    ) -> Result<
        Spawned,
        ReviveError<VmId, <Backend as LoadSnapshot>::Error, rmp_serde::decode::Error>,
    >
    where
        EffectsHandler: EffectHandler<Effect = Interpreter::Effect>
            + PromiseSettler<Value = Value::ReadyValue>
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
        Value::ReadyValue: core::fmt::Debug,
    {
        let data = self
            .backend
            .load_snapshot(&vm_id)
            .await
            .map_err(ReviveError::Load)?;

        let mut de = rmp_serde::Deserializer::new(&data[..]);
        let runtime =
            Runtime::from_snapshot(interpreter, executable, &mut de).map_err(|error| {
                ReviveError::DeserializationFailed {
                    vm_id: vm_id.clone(),
                    error,
                }
            })?;

        self.spawn(
            vm_id,
            ManagedRuntime::from_runtime(runtime),
            effects_handler,
        )
        .await
        .map_err(ReviveError::Spawn)
    }
}
