//! VM driver thread spawning.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

/// The outcome of awaiting a VM eviction; see [`Spawned::evicted`].
#[derive(Debug)]
pub enum Evicted<DriverError> {
    /// The driver exited with this error; the caller took it and now
    /// owns handling it.
    DriverError(DriverError),

    /// The exit error was already taken by a competing caller; that
    /// caller owns handling it.
    HandledElsewhere,
}

/// A spawned VM handle that exposes the access to the VM.
#[must_use = "driver thread is aborted when this handle is dropped"]
pub struct Spawned<DriverError> {
    /// Gracefully cancels the driver loop.
    driver_loop_cancel: CancellationToken,

    /// A handle to allow joining on the driver completion task.
    ///
    /// That's the one that waits for the driver to complete.
    driver_completion_task: Option<tokio::task::JoinHandle<()>>,

    /// Delivers the exit error reported by the driver completion task;
    /// see [`Spawned::evicted`].
    driver_error: crate::once_receiver::OnceReceiver<DriverError>,

    /// Opaque handles kept alive while this VM is running.
    #[allow(
        unused,
        reason = "these handles are kept around for the drop semantics, \
                    they don't actually have to be used"
    )]
    keepalive_handles: Vec<Box<dyn Send + Sync>>,
}

impl<DriverError> Spawned<DriverError> {
    /// Wait for the VM driver thread to exit (the VM is evicted) and
    /// take the exit error.
    ///
    /// The error is handed out exactly once: the first call to complete
    /// receives [`Evicted::DriverError`], and every call after that receives
    /// [`Evicted::HandledElsewhere`].
    ///
    /// # Cancellation safety
    ///
    /// This method is cancellation-safe: if the future is dropped
    /// before completing, the error is retained and remains available
    /// to the next caller.
    pub async fn evicted(&self) -> Evicted<DriverError> {
        match self.driver_error.recv().await {
            Some(Ok(error)) => Evicted::DriverError(error),
            // The completion task is never aborted (dropping the
            // `Spawned` merely detaches it) and every statement between
            // its spawn and the send is infallible, so the sender
            // cannot be dropped without sending — short of runtime
            // teardown, during which no caller is left running to
            // observe it.
            Some(Err(_)) => unreachable!("driver completion task gone without reporting"),
            None => Evicted::HandledElsewhere,
        }
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

impl<DriverError> Drop for Spawned<DriverError> {
    fn drop(&mut self) {
        self.driver_loop_cancel.cancel();
    }
}

/// The exit error a spawned VM driver reports, as computed from the
/// higher-level types the VM is spawned with.
pub type ErrorFor<Interpreter, Codec, Persister, Effector> = waymark_vm_driver_thread::Error<
    waymark_vm_driver::ErrorFor<Interpreter, Arc<Codec>, Persister, Effector>,
>;

/// Spawn a new VM runtime on a dedicated OS thread.
#[tracing::instrument(skip_all)]
pub(crate) async fn spawn<Codec, Executable, Interpreter, Value, Effector, Persister>(
    codec: Arc<Codec>,
    runtime: waymark_vm_runtime::Runtime<Executable, Interpreter, Value>,
    effector: Effector,
    persister: Persister,
    keepalive_handles: Vec<Box<dyn Send + Sync>>,
) -> Spawned<ErrorFor<Interpreter, Codec, Persister, Effector>>
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
    for<'view, 'runtime> <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'view>:
        waymark_vm_runtime_view_capture::CaptureRuntimeView<
                'view,
                waymark_vm_runtime_core::FullRuntimeView<
                    'runtime,
                    Executable,
                    Executable::FunctionId,
                    Executable::StateId,
                    Value,
                >,
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
    let (error_tx, error_rx) = tokio::sync::oneshot::channel();
    let driver = waymark_vm_driver_thread::spawn(waymark_vm_driver::Params {
        runtime,
        effector,
        persister,
        codec,
        cancel: cancel.clone(),
    });

    let completion = tokio::spawn({
        // The send signals the eviction; if the task is aborted before
        // the driver exits, dropping the sender signals it instead.
        async move {
            let Err(error) = driver.await;
            let _ = error_tx.send(error);
        }
        .in_current_span()
    });

    Spawned {
        driver_loop_cancel: cancel,
        driver_completion_task: Some(completion),
        driver_error: crate::once_receiver::OnceReceiver::new(error_rx),
        keepalive_handles,
    }
}
