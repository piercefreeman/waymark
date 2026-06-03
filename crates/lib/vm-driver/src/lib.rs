//! Driver loop for a VM runtime.
//!
//! This crate ties [`Runtime`] to external async channels by forwarding emitted
//! effects to a caller-provided sender and resolving pending promises from a
//! caller-provided promise-resolution receiver.

#![warn(missing_docs)]

use waymark_vm_runtime::{FrameFor, Runtime};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// A promise settlement received by the driver.
pub enum PromiseResolution<Value> {
    /// Resolve the promise successfully.
    Resolved(Value),

    /// Reject the promise with an exception.
    Rejected(waymark_vm_runtime_exception::Exception<Value>),
}

/// Errors returned by the driver loop.
#[derive(Debug)]
pub enum Error<ExecutionError, Value> {
    /// A VM step failed while executing an instruction.
    Step(waymark_vm_runtime::step::Error<ExecutionError>),

    /// The effect receiver side was dropped.
    EffectSenderClosed,

    /// The promise-resolution sender side was dropped.
    PromiseResolutionReceiverClosed,

    /// Resolving a promise failed.
    ResolvingPromise(waymark_vm_runtime_core::ResolvePromiseError<Value>),

    /// Rejecting a promise failed.
    RejectingPromise(waymark_vm_runtime_core::RejectPromiseError<Value>),
}

/// Inputs required to run the driver loop.
pub struct Params<Executable, Interpreter, Value>
where
    Executable: waymark_vm_executable::FunctionStates,
    Interpreter: waymark_vm_interpreter::Interpreter<Frame = FrameFor<Executable, Value>>,
    Value: waymark_vm_runtime_promise_core::Resolvable,
{
    /// Runtime instance to drive.
    pub runtime: Runtime<Executable, Interpreter, Value>,

    /// Channel used to publish effects emitted by the runtime.
    pub effects_tx: tokio::sync::mpsc::Sender<Interpreter::Effect>,

    /// Channel used to receive promise resolutions for pending promises.
    pub promise_resolutions_rx:
        tokio::sync::mpsc::Receiver<(PromiseStateId, PromiseResolution<Value::ReadyValue>)>,
}

/// Drive the runtime until the loop terminates with an error.
///
/// The driver repeatedly executes the runtime, forwards emitted effects to
/// [`Params::effects_tx`], and settles pending promises with values received
/// from [`Params::promise_resolutions_rx`]. The function only returns when one
/// of those operations fails.
pub async fn run<Executable, Interpreter, Value>(
    params: Params<Executable, Interpreter, Value>,
) -> Result<core::convert::Infallible, Error<Interpreter::Error, Value::ReadyValue>>
where
    Executable: waymark_vm_executable::InstructionsProvider,
    Executable::FunctionId: Copy,
    Executable::StateId: Copy + PartialEq,
    Executable: 'static,
    Interpreter: waymark_vm_interpreter::Interpreter<
            Frame = FrameFor<Executable, Value>,
            Instruction = Executable::Instruction,
        >,
    Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
            Executable,
            Executable::FunctionId,
            Executable::StateId,
            Value,
            RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'r>,
        >,
    Value: 'static,
    Value: Clone,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    // Debug
    Interpreter::Instruction: core::fmt::Debug,
    Interpreter::Effect: core::fmt::Debug,
    Value: core::fmt::Debug,
    Value::ReadyValue: core::fmt::Debug,
{
    let Params {
        mut runtime,
        effects_tx,
        mut promise_resolutions_rx,
    } = params;

    loop {
        match runtime.run() {
            Ok(effect) => {
                tracing::info!(?effect, "effect");
                if effects_tx.send(effect).await.is_err() {
                    return Err(Error::EffectSenderClosed);
                }
            }
            Err(waymark_vm_runtime::RunError::NoReadyFrame) => {
                let (promise_state_id, resolution) = promise_resolutions_rx
                    .recv()
                    .await
                    .ok_or(Error::PromiseResolutionReceiverClosed)?;

                match resolution {
                    PromiseResolution::Resolved(value) => {
                        tracing::info!(?promise_state_id, ?value, "promise resolution");
                        runtime
                            .resolve_promise(promise_state_id, value)
                            .map_err(Error::ResolvingPromise)?;
                    }
                    PromiseResolution::Rejected(exception) => {
                        tracing::info!(?promise_state_id, ?exception, "promise rejection");
                        runtime
                            .reject_promise(promise_state_id, exception)
                            .map_err(Error::RejectingPromise)?;
                    }
                }
            }
            Err(waymark_vm_runtime::RunError::Step(error)) => return Err(Error::Step(error)),
        };
    }
}
