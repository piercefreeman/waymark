//! VM driver thread spawning.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

/// A spawned VM handle that exposes the access to the VM.
#[must_use = "driver thread is aborted when this handle is dropped"]
pub struct Spawned {
    /// Gracefully cancels the driver loop.
    driver_loop_cancel: CancellationToken,

    /// A handle to allow joining on the driver completion task.
    ///
    /// That's the one that waits for the driver to complete.
    driver_completion_task: Option<tokio::task::JoinHandle<()>>,

    /// Cancelled when the driver thread exits (the VM is evicted).
    driver_evicted: CancellationToken,

    /// Opaque handles kept alive while this VM is running.
    #[allow(
        unused,
        reason = "these handles are kept around for the drop semantics, \
                    they don't actually have to be used"
    )]
    keepalive_handles: Vec<Box<dyn Send + Sync>>,
}

impl Spawned {
    /// Returns a future that resolves when the VM driver thread exits
    /// (the VM is evicted).
    pub fn evicted(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.driver_evicted.cancelled()
    }

    /// Trigger the graceful eviction of the VM.
    ///
    /// The VM driver loop is cancelled (not the loop future itself, but
    /// the token), and will stop the execution when the loop picks
    /// the cancellation up.
    pub fn trigger_eviction(&self) {
        self.driver_loop_cancel.cancel();
    }

    /// Gracefully shut down the VM, waiting for the driver thread to exit.
    ///
    /// # Cancellation safety
    ///
    /// This method is cancellation-safe. If the future is dropped, the
    /// shutdown signal has already been sent (via the cancellation token)
    /// before any `.await` point. The driver thread will continue shutting
    /// down asynchronously.
    pub async fn shutdown(mut self) {
        self.driver_loop_cancel.cancel();

        let completion_task = self.driver_completion_task.take().unwrap();

        let completion_result = completion_task.await;

        match completion_result {
            Ok(()) => {}                         // fallthrough to disable abort-on-drop
            Err(err) if err.is_cancelled() => {} // fallthrough to disable abort-on-drop
            Err(err) => std::panic::resume_unwind(err.into_panic()), // unwind and trigger abort-on-drop
        }
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        self.driver_loop_cancel.cancel();
    }
}

/// Spawn a new VM runtime on a dedicated OS thread.
#[tracing::instrument(skip_all)]
pub(crate) async fn spawn<Codec, Executable, Interpreter, Value, Effector, Persister>(
    codec: Arc<Codec>,
    runtime: waymark_vm_runtime::Runtime<Executable, Interpreter, Value>,
    effector: Effector,
    persister: Persister,
    keepalive_handles: Vec<Box<dyn Send + Sync>>,
) -> Spawned
where
    Codec: waymark_vm_codec_core::SerializerProvider<Ok = ()>,
    Codec: Send + Sync + 'static,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error: Send,
    Effector: waymark_vm_driver_core::EffectHandler<Effect = Interpreter::Effect>,
    Effector: waymark_vm_driver_core::PromiseSettler<Value = Value::ReadyValue, Ack: Send>,
    Effector: Send + 'static,
    Executable: waymark_vm_executable::InstructionsProvider + Send + 'static,
    Executable::FunctionId: Copy + Send,
    Executable::FunctionId: serde::Serialize,
    Executable::StateId: Copy + PartialEq + Send,
    Executable::StateId: serde::Serialize,
    Executable: waymark_vm_executable::FunctionStates + Send,
    Interpreter: waymark_vm_interpreter::Interpreter<
            Frame = waymark_vm_runtime::FrameFor<Executable, Value>,
            Instruction = Executable::Instruction,
        >,
    Interpreter: Send + 'static,
    Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
            Executable,
            Executable::FunctionId,
            Executable::StateId,
            Value,
            RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'r>,
        >,
    Value: Clone + Send + 'static,
    Value: serde::Serialize,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Interpreter::Error: core::fmt::Debug + Send,
    Interpreter::Effect: core::fmt::Debug + Send,
    Interpreter::Instruction: core::fmt::Debug,
    Value: core::fmt::Debug,
    Value::ReadyValue: core::fmt::Debug + Send,
    Persister: waymark_vm_driver_core::SnapshotPersister + Send + 'static,
    Persister::Error: Send,
    <Effector as waymark_vm_driver_core::EffectHandler>::Error: Send,
    <Effector as waymark_vm_driver_core::PromiseSettler>::Error: Send,
{
    let cancel = CancellationToken::new();
    let driver_evicted = CancellationToken::new();
    let driver = waymark_vm_driver_thread::spawn(waymark_vm_driver::Params {
        runtime,
        effector,
        persister,
        codec,
        cancel: cancel.clone(),
    });

    let completion = tokio::spawn({
        let driver_evicted = driver_evicted.clone();
        {
            async move {
                let _driver_evicted = driver_evicted.drop_guard();
                let Err(error) = driver.await;
                match error {
                    waymark_vm_driver_thread::Error::Driver(error) => {
                        tracing::error!(?error, "vm driver terminated");
                    }
                    waymark_vm_driver_thread::Error::Thread(error) => {
                        tracing::error!(?error, "vm thread panicked");
                    }
                }
                tracing::debug!("vm driver thread exited");
            }
        }
        .in_current_span()
    });

    Spawned {
        driver_loop_cancel: cancel,
        driver_completion_task: Some(completion),
        driver_evicted,
        keepalive_handles,
    }
}
