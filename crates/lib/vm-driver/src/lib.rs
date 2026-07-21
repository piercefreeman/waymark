//! Driver loop for a VM runtime.
//!
//! This crate ties [`Runtime`] to external async channels by forwarding emitted
//! effects to a caller-provided sender and resolving pending promises from a
//! caller-provided promise-resolution receiver.

#![warn(missing_docs)]

mod snapshot;

use nonempty_collections::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use tokio_util::sync::CancellationToken;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime::{FrameFor, Runtime};
use waymark_vm_runtime_core::ResolvePromiseError;

/// Errors returned by the driver loop.
#[derive(Debug)]
pub enum Error<
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> {
    /// A VM step failed while executing an instruction.
    Step(waymark_vm_runtime::step::Error<ExecutionError>),

    /// The runtime state has no ready frames or waiting promises.
    ///
    /// This means the runtime will be unable to ever progress further and
    /// can safely be dropped.
    NoReadyFramesOrWaitingPromises,

    /// The snapshot serialization has failed.
    SnapshotSerialization(SnapshotSerializationError),

    /// The snapshot persistence has failed.
    SnapshotPersistence(SnapshotPersistenceError),

    /// The effect handling has failed.
    EffectHandling(EffectHandlingError),

    /// Getting promise settlements has failed.
    GettingPromiseSettlements(GettingPromiseSettlementsError),

    /// The driver was cancelled via its [`CancellationToken`].
    Cancelled,
}

/// Convenience alias for [`Error`] that computes the concrete error
/// types from the higher-level type parameters used by [`run`].
pub type ErrorFor<Interpreter, Codec, Persister, Effector> = Error<
    <Interpreter as waymark_vm_interpreter::Interpreter>::Error,
    <Codec as waymark_vm_codec_core::SerializerProvider>::Error,
    <Persister as waymark_vm_driver_core::SnapshotPersister>::Error,
    <Effector as waymark_vm_driver_core::EffectHandler>::Error,
    <Effector as waymark_vm_driver_core::PromiseSettler>::Error,
>;

/// Inputs required to run the driver loop.
pub struct Params<Executable, Interpreter, Value, Effector, Persister, Codec>
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

    /// Snapshot codec.
    pub codec: Codec,

    /// Cancel token for graceful shutdown.
    pub cancel: CancellationToken,
}

/// Drive the runtime until cancelled or a fatal error occurs.
///
/// The driver repeatedly executes the runtime, forwards emitted effects to
/// the effector, and settles pending promises with values received from the
/// effector. Returns [`Error::Cancelled`] when the [`CancellationToken`] is
/// triggered, or another error variant when a fatal error occurs.
pub async fn run<Executable, Interpreter, Value, Effector, Persister, Codec>(
    params: Params<Executable, Interpreter, Value, Effector, Persister, Codec>,
) -> Result<core::convert::Infallible, ErrorFor<Interpreter, Codec, Persister, Effector>>
where
    Executable: waymark_vm_executable::InstructionsProvider,
    Executable::FunctionId: Copy + serde::Serialize,
    Executable::StateId: Copy + PartialEq + serde::Serialize,
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
    Codec: waymark_vm_codec_core::SerializerProvider<Ok = ()>,
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
        codec,
        cancel,
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
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }

        // Read all the buffered promise settlements.
        for settlement in buffered_settlements.drain(..) {
            let PromiseSettlement {
                promise_state_id,
                resolution,
                ack,
            } = settlement;

            // Stale settlements are normal under at-least-once
            // delivery: a redelivery of an applied settlement finds its
            // promise already settled, or — once settled promise states
            // are garbage collected — not present at all.  Either way
            // there is nothing to apply and the settlement is still
            // acked below: the ack is what removes the durable record,
            // and without it it would redeliver forever.  Benign by
            // enumeration, not by default: the matches are exhaustive
            // over the two known variants, so a new error variant is a
            // compile error here, forcing a conscious classification.
            match resolution {
                PromiseResolution::Resolved(value) => {
                    tracing::info!(?promise_state_id, ?value, "promise resolution");
                    match runtime.resolve_promise(promise_state_id, value) {
                        Ok(()) => {}
                        Err(
                            error @ (ResolvePromiseError::AlreadySettled(_)
                            | ResolvePromiseError::PromiseStateNotFound(_)),
                        ) => {
                            tracing::info!(
                                ?promise_state_id,
                                ?error,
                                "stale promise resolution ignored"
                            );
                        }
                    }
                }
                PromiseResolution::Rejected(exception) => {
                    tracing::info!(?promise_state_id, ?exception, "promise rejection");
                    match runtime.reject_promise(promise_state_id, exception) {
                        Ok(()) => {}
                        Err(
                            error @ (ResolvePromiseError::AlreadySettled(_)
                            | ResolvePromiseError::PromiseStateNotFound(_)),
                        ) => {
                            tracing::info!(
                                ?promise_state_id,
                                ?error,
                                "stale promise rejection ignored"
                            );
                        }
                    }
                }
            }

            promise_settlement_acks.push(ack);
        }

        // Check if there are some promise settlements we need to persist.
        should_persist = should_persist || !promise_settlement_acks.is_empty();

        // Persist the runtime snapshot if we should.
        if should_persist {
            let data = snapshot_buffer
                .write_with(|buf| codec.with_serializer(buf, |ser| runtime.snapshot(ser)))
                .map_err(Error::SnapshotSerialization)?;
            persister
                .persist_snapshot(&data)
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
            Ok(emitted_effect) => {
                tracing::info!(?emitted_effect.effect, %emitted_effect.number, "effect");

                effector
                    .handle_effect(emitted_effect)
                    .await
                    .map_err(Error::EffectHandling)?;

                should_persist = true;
            }
            Err(waymark_vm_runtime::RunError::NoReadyFrame) => {
                let waiting_promise_state_ids = runtime
                    .waiting_promise_state_ids()
                    .try_into_nonempty_iter()
                    .ok_or(Error::NoReadyFramesOrWaitingPromises)?;
                let waiting_promise_state_ids: NEVec<_> = waiting_promise_state_ids.collect();

                let settlements = cancel
                    .run_until_cancelled(
                        effector.get_promise_settlements(waiting_promise_state_ids),
                    )
                    .await
                    .ok_or(Error::Cancelled)?
                    .map_err(Error::GettingPromiseSettlements)?;

                let mut settlements: Vec<_> = settlements.into();
                buffered_settlements.append(settlements.as_mut());
            }
            Err(waymark_vm_runtime::RunError::Step(error)) => return Err(Error::Step(error)),
        };
    }
}
