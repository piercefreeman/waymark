//! Driver loop for a VM runtime.
//!
//! This crate ties [`Runtime`] to external async channels by forwarding emitted
//! effects to a caller-provided sender and resolving pending promises from a
//! caller-provided promise-resolution receiver.

#![warn(missing_docs)]

mod snapshot;

use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime::{FrameFor, Runtime};

/// Errors returned by the driver loop.
#[derive(Debug)]
pub enum Error<
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> {
    /// A VM step failed while executing an instruction.
    Step(waymark_vm_runtime::step::Error<ExecutionError>),

    /// The snapshot serialization has failed.
    SnapshotSerialization(SnapshotSerializationError),

    /// The snapshot persistence has failed.
    SnapshotPersistence(SnapshotPersistenceError),

    /// The effect handling has failed.
    EffectHandling(EffectHandlingError),

    /// Getting promise settlements has failed.
    GettingPromiseSettlements(GettingPromiseSettlementsError),

    /// Resolving a promise failed.
    ResolvingPromise(waymark_vm_runtime_core::ResolvePromiseError<Value>),

    /// Rejecting a promise failed.
    RejectingPromise(waymark_vm_runtime_core::RejectPromiseError<Value>),
}

/// Inputs required to run the driver loop.
pub struct Params<Executable, Interpreter, Value, Effector, Persister>
where
    Executable: waymark_vm_executable::FunctionStates,
    Interpreter: waymark_vm_interpreter::Interpreter<Frame = FrameFor<Executable, Value>>,
{
    /// Runtime instance to drive.
    pub runtime: Runtime<Executable, Interpreter, Value>,

    /// Handler for effects and promise settlements.
    pub effector: Effector,

    /// Snapshot persistence.
    pub persister: Persister,
}

/// Drive the runtime until the loop terminates with an error.
///
/// The driver repeatedly executes the runtime, forwards emitted effects to
/// [`Params::effects_tx`], and settles pending promises with values received
/// from [`Params::promise_resolutions_rx`]. The function only returns when one
/// of those operations fails.
pub async fn run<Executable, Interpreter, Value, Effector, Persister>(
    params: Params<Executable, Interpreter, Value, Effector, Persister>,
) -> Result<
    core::convert::Infallible,
    Error<
        Value::ReadyValue,
        Interpreter::Error,
        rmp_serde::encode::Error,
        Persister::Error,
        <Effector as waymark_vm_driver_core::EffectHandler>::Error,
        <Effector as waymark_vm_driver_core::PromiseSettler>::Error,
    >,
>
where
    Executable: waymark_vm_executable::InstructionsProvider,
    Executable::FunctionId: Copy + serde::Serialize,
    Executable::StateId: Copy + PartialEq + serde::Serialize,
    Executable: waymark_vm_executable::FunctionStates,
    Executable: 'static,
    Interpreter: waymark_vm_interpreter::Interpreter<
            Frame = waymark_vm_runtime::FrameFor<Executable, Value>,
            Instruction = Executable::Instruction,
        >,
    Interpreter: for<'r> waymark_vm_runtime_core::CaptureRuntimeView<
            Executable,
            Executable::FunctionId,
            Executable::StateId,
            Value,
            RuntimeView<'r> = <Interpreter as waymark_vm_interpreter::Interpreter>::RuntimeView<'r>,
        >,
    Value: Clone + 'static + serde::Serialize,
    Value: waymark_vm_runtime_promise_core::Resolvable,
    Effector: waymark_vm_driver_core::EffectHandler<Effect = Interpreter::Effect>,
    Effector: waymark_vm_driver_core::PromiseSettler<Value = Value::ReadyValue>,
    Persister: waymark_vm_driver_core::SnapshotPersister,
    // Debug
    Interpreter::Instruction: core::fmt::Debug,
    Interpreter::Effect: core::fmt::Debug,
    Value: core::fmt::Debug,
    Value::ReadyValue: core::fmt::Debug,
{
    let Params {
        mut runtime,
        mut effector,
        persister,
    } = params;

    // The list of buffered promise settlements,
    // kept intact across loop iterations.
    let mut buffered_settlements =
        Vec::<waymark_vm_driver_core::PromiseSettlementFor<Effector>>::new();

    // Reusable promise settlement acks buffer.
    // Used only within each iteration and thus only here to keep
    // the internal capacity/allocation around.
    let mut promise_settlement_acks =
        Vec::<<Effector as waymark_vm_driver_core::PromiseSettler>::Ack>::new();

    // Reusable snapshot buffer.
    // Keeps the memory allocation roughly fitting the snapshot.
    let mut snapshot_buffer = snapshot::Buffer::default();

    // Track whether we need to persist the runtime this tick or not.
    let mut should_persist = false;

    loop {
        // Read all the buffered promise settlements.
        for settlement in buffered_settlements.drain(..) {
            let PromiseSettlement {
                promise_state_id,
                resolution,
                ack,
            } = settlement;

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

            promise_settlement_acks.push(ack);
        }

        // Check if there are some promise settlements we need to persist.
        should_persist = should_persist || !promise_settlement_acks.is_empty();

        // Persist the runtime snapshot if we should.
        if should_persist {
            let mut buffer = snapshot_buffer.r#use();
            runtime
                .snapshot(buffer.serializer())
                .map_err(Error::SnapshotSerialization)?;
            persister
                .persist_snapshot(buffer.data())
                .await
                .map_err(Error::SnapshotPersistence)?;
        }

        // Acknowledge all promise settlements.
        for ack in promise_settlement_acks.drain(..) {
            use waymark_vm_driver_core::PromiseSettlementAck as _;
            ack.acknowledge_promise_settlement();
        }

        // Then, execute all ready frames until none are left and we suspend
        // or there is an effect.
        match runtime.run() {
            Ok(effect) => {
                tracing::info!(?effect, "effect");
                effector
                    .handle_effect(effect)
                    .await
                    .map_err(Error::EffectHandling)?;

                should_persist = true;
            }
            Err(waymark_vm_runtime::RunError::NoReadyFrame) => {
                let settlements = effector
                    .get_promise_settlements()
                    .await
                    .map_err(Error::GettingPromiseSettlements)?;

                let mut settlements: Vec<_> = settlements.into();
                buffered_settlements.append(settlements.as_mut());
            }
            Err(waymark_vm_runtime::RunError::Step(error)) => return Err(Error::Step(error)),
        };
    }
}
