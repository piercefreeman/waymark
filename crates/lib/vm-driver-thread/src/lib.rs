//! Thread-spawning wrapper for the VM driver loop.
//!
//! This crate runs [`driver::run`] on a dedicated OS thread so the
//! driver can use async operations without ever being scheduled on the tokio
//! async worker pool — preventing any blocking behaviour inside the driver
//! from starving the pool.

#![warn(missing_docs)]

use std::pin::Pin;

pub use waymark_vm_driver as driver;

use tracing::Instrument as _;

/// Errors returned by [`spawn`].
#[derive(Debug)]
pub enum Error<DriverError> {
    /// The driver loop terminated with an error.
    Driver(DriverError),

    /// The OS thread panicked.
    Thread(tokio::task::JoinError),
}

/// Convenience alias for [`Handle`] that computes the concrete type
/// parameters from the higher-level types used by [`spawn`].
pub type HandleFor<Value, Interpreter, Codec, Persister, Effector> = Handle<
    <Value as waymark_vm_runtime_promise_core::Resolvable>::ReadyValue,
    <Interpreter as waymark_vm_interpreter::Interpreter>::Error,
    <Codec as waymark_vm_codec_core::SnapshotSerializer>::Error,
    <Persister as waymark_vm_driver_core::SnapshotPersister>::Error,
    <Effector as waymark_vm_driver_core::EffectHandler>::Error,
    <Effector as waymark_vm_driver_core::PromiseSettler>::Error,
>;

/// Spawn the driver loop onto a dedicated OS thread.
///
/// The driver runs on its own OS thread with full async support, but never
/// contends with the tokio worker pool. This means any blocking operations
/// inside the driver (or blocking-like behaviour from the VM runtime) won't
/// starve other async tasks running on the pool.
///
/// Current tracing span is carried over to the driver thread.
pub fn spawn<Executable, Interpreter, Value, Effector, Persister, Codec>(
    params: driver::Params<Executable, Interpreter, Value, Effector, Persister, Codec>,
) -> HandleFor<Value, Interpreter, Codec, Persister, Effector>
where
    Executable: waymark_vm_executable::InstructionsProvider + Send + 'static,
    Executable::FunctionId: Copy + serde::Serialize + Send,
    Executable::StateId: Copy + PartialEq + serde::Serialize + Send,
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
            RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'r>,
        >,
    Value: Clone + Send + 'static + serde::Serialize,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Effector: waymark_vm_driver_core::EffectHandler<Effect = Interpreter::Effect> + Send + 'static,
    Effector: waymark_vm_driver_core::PromiseSettler<Value = Value::ReadyValue, Ack: Send> + Send,
    Persister: waymark_vm_driver_core::SnapshotPersister + Send + 'static,
    Codec: waymark_vm_codec_core::SnapshotSerializer + Send + 'static,
    // Send bounds for associated error types
    <Codec as waymark_vm_codec_core::SnapshotSerializer>::Error: Send,
    <Persister as waymark_vm_driver_core::SnapshotPersister>::Error: Send,
    <Effector as waymark_vm_driver_core::EffectHandler>::Error: Send,
    <Effector as waymark_vm_driver_core::PromiseSettler>::Error: Send,
    // Debug
    Interpreter::Instruction: core::fmt::Debug,
    Interpreter::Effect: core::fmt::Debug + Send,
    Interpreter::Error: core::fmt::Debug + Send,
    Value: core::fmt::Debug,
    Value::ReadyValue: core::fmt::Debug + Send,
{
    let task = waymark_blocking_future::spawn_thread(driver::run(params).in_current_span());

    Handle { task }
}

/// Handle to a driver loop running on a dedicated OS thread.
///
/// Implements [`IntoFuture`] so it can be `.await`ed to wait for the driver
/// to finish. On completion, returns either the driver's terminal error or
/// a thread-level panic error.
pub struct Handle<
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> {
    task: DriverTaskJoinHandle<
        Value,
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >,
}

/// Join handle for the OS thread running the driver loop.
type DriverTaskJoinHandle<
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> = tokio::task::JoinHandle<
    Result<
        core::convert::Infallible,
        driver::Error<
            Value,
            ExecutionError,
            SnapshotSerializationError,
            SnapshotPersistenceError,
            EffectHandlingError,
            GettingPromiseSettlementsError,
        >,
    >,
>;

impl<
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> IntoFuture
    for Handle<
        Value,
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >
where
    Value: core::fmt::Debug + Send + 'static,
    ExecutionError: core::fmt::Debug + Send + 'static,
    SnapshotSerializationError: core::fmt::Debug + Send + 'static,
    SnapshotPersistenceError: core::fmt::Debug + Send + 'static,
    EffectHandlingError: core::fmt::Debug + Send + 'static,
    GettingPromiseSettlementsError: core::fmt::Debug + Send + 'static,
{
    type Output = Result<
        core::convert::Infallible,
        Error<
            driver::Error<
                Value,
                ExecutionError,
                SnapshotSerializationError,
                SnapshotPersistenceError,
                EffectHandlingError,
                GettingPromiseSettlementsError,
            >,
        >,
    >;

    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            Err(match self.task.await {
                Err(error) => {
                    tracing::error!(?error, "vm driver thread panicked");
                    Error::Thread(error)
                }
                Ok(Err(error)) => {
                    tracing::error!(?error, "vm driver terminated");
                    Error::Driver(error)
                }
            })
        })
    }
}

impl<
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
>
    Handle<
        Value,
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >
{
    /// Abort the driver thread, cancelling the task.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Return an [`AbortHandle`](tokio::task::AbortHandle) that can be used
    /// to abort the driver thread from outside this handle.
    pub fn abort_handle(&self) -> tokio::task::AbortHandle {
        self.task.abort_handle()
    }
}
