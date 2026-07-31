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
    Thread(Box<dyn std::any::Any + Send + 'static>),
}

/// Convenience alias for [`Handle`] that computes the concrete type
/// parameters from the higher-level types used by [`spawn`].
pub type HandleFor<Interpreter, Codec, Persister, Effector> = Handle<
    <Interpreter as waymark_vm_interpreter::Interpreter>::Error,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error,
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
) -> HandleFor<Interpreter, Codec, Persister, Effector>
where
    // Executable
    Executable: waymark_vm_executable::InstructionsProvider + Send + 'static,
    Executable::FunctionId: Copy + serde::Serialize + Send,
    Executable::StateId: Copy + PartialEq + serde::Serialize + Send,
    // Interpreter
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
    Interpreter::Instruction: core::fmt::Debug,
    Interpreter::Effect: core::fmt::Debug + Send,
    Interpreter::Error: core::fmt::Debug + Send,
    // Value
    Value: Clone + Send + 'static,
    Value: serde::Serialize,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Value: core::fmt::Debug,
    Value::ReadyValue: core::fmt::Debug + Send,
    // Effector
    Effector: waymark_vm_driver_core::EffectHandler<Effect = Interpreter::Effect> + Send + 'static,
    Effector: waymark_vm_driver_core::PromiseSettler<Value = Value::ReadyValue, Ack: Send> + Send,
    <Effector as waymark_vm_driver_core::EffectHandler>::Error: Send,
    <Effector as waymark_vm_driver_core::PromiseSettler>::Error: Send,
    // Persister
    Persister: waymark_vm_driver_core::SnapshotPersister + Send + 'static,
    <Persister as waymark_vm_driver_core::SnapshotPersister>::Error: Send,
    // Codec
    Codec: waymark_vm_codec_core::SerializerProvider<Ok = ()> + Send + 'static,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error: Send,
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
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> {
    task: DriverTaskJoinHandle<
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >,
}

/// Join handle for the OS thread running the driver loop.
type DriverTaskJoinHandle<
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> = waymark_blocking_future::JoinHandle<
    Result<
        core::convert::Infallible,
        driver::Error<
            ExecutionError,
            SnapshotSerializationError,
            SnapshotPersistenceError,
            EffectHandlingError,
            GettingPromiseSettlementsError,
        >,
    >,
>;

impl<
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> IntoFuture
    for Handle<
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >
where
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
