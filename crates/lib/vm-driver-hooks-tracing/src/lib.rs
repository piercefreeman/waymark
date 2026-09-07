//! VM driver hooks that log the run.
//!
//! The run's start, its snapshots and, above all, its end: the driver's
//! error is matched here, so an expected exit logs at debug and a failure
//! at error. Effects and settlements are not logged here — the driver loop
//! logs them itself as they happen.
//!
//! The hooks carry no identity: the driver runs inside the span of whoever
//! started it, and every line here inherits that span's fields.

#![warn(missing_docs)]

use std::marker::PhantomData;

use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Hooks that log the run.
///
/// The type parameters only pin the effect, value and error types the hooks
/// are declared over; `Tracing` is `Send`, `Sync` and `'static` regardless
/// of them.
pub struct Tracing<Effect, Value, DriverError>(PhantomData<Parameters<Effect, Value, DriverError>>);

/// The hooks' type parameters, held as a function pointer type so that
/// [`Tracing`] stays `Send`, `Sync` and `'static` whatever they are.
type Parameters<Effect, Value, DriverError> = fn() -> (Effect, Value, DriverError);

impl<Effect, Value, DriverError> Tracing<Effect, Value, DriverError> {
    /// Create the logging hooks.
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Effect, Value, DriverError> Default for Tracing<Effect, Value, DriverError> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Effect, Value, DriverError> waymark_vm_driver_hooks::effect_emitted::HasEffect
    for Tracing<Effect, Value, DriverError>
{
    type Effect = Effect;
}

impl<Effect, Value, DriverError> waymark_vm_driver_hooks::promise_settled::HasValue
    for Tracing<Effect, Value, DriverError>
{
    type Value = Value;
}

impl<
    Effect,
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> waymark_vm_driver_hooks::vm_stopped::HasError
    for Tracing<
        Effect,
        Value,
        waymark_vm_driver::Error<
            ExecutionError,
            SnapshotSerializationError,
            SnapshotPersistenceError,
            EffectHandlingError,
            GettingPromiseSettlementsError,
        >,
    >
{
    type Error = waymark_vm_driver::Error<
        ExecutionError,
        SnapshotSerializationError,
        SnapshotPersistenceError,
        EffectHandlingError,
        GettingPromiseSettlementsError,
    >;
}

impl<Effect, Value, DriverError> waymark_vm_driver_hooks::VmStarted
    for Tracing<Effect, Value, DriverError>
{
    fn vm_started(&self) {
        tracing::debug!("vm driver started");
    }
}

impl<Effect, Value, DriverError> waymark_vm_driver_hooks::EffectEmitted
    for Tracing<Effect, Value, DriverError>
{
    fn effect_emitted(&self, _number: EffectNumber, _effect: &Self::Effect) {}
}

impl<Effect, Value, DriverError> waymark_vm_driver_hooks::PromiseSettled
    for Tracing<Effect, Value, DriverError>
{
    fn promise_settled(
        &self,
        _promise_state_id: PromiseStateId,
        _resolution: &PromiseResolution<Self::Value>,
    ) {
    }
}

impl<Effect, Value, DriverError> waymark_vm_driver_hooks::SnapshotPersisted
    for Tracing<Effect, Value, DriverError>
{
    fn snapshot_persisted(&self, size_in_bytes: usize) {
        tracing::debug!(size_in_bytes, "vm snapshot persisted");
    }
}

impl<
    Effect,
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> waymark_vm_driver_hooks::VmStopped
    for Tracing<
        Effect,
        Value,
        waymark_vm_driver::Error<
            ExecutionError,
            SnapshotSerializationError,
            SnapshotPersistenceError,
            EffectHandlingError,
            GettingPromiseSettlementsError,
        >,
    >
where
    ExecutionError: core::fmt::Debug,
    SnapshotSerializationError: core::fmt::Debug,
    SnapshotPersistenceError: core::fmt::Debug,
    EffectHandlingError: core::fmt::Debug,
    GettingPromiseSettlementsError: core::fmt::Debug,
{
    fn vm_stopped(&self, error: &Self::Error) {
        match error {
            waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises => {
                tracing::debug!("vm driver exhausted: no ready frames or waiting promises");
            }
            waymark_vm_driver::Error::Cancelled => {
                tracing::debug!("vm driver cancelled");
            }
            waymark_vm_driver::Error::Step(_)
            | waymark_vm_driver::Error::SnapshotSerialization(_)
            | waymark_vm_driver::Error::SnapshotPersistence(_)
            | waymark_vm_driver::Error::EffectHandling(_)
            | waymark_vm_driver::Error::GettingPromiseSettlements(_) => {
                tracing::error!(?error, "vm driver failed");
            }
        }
    }
}
