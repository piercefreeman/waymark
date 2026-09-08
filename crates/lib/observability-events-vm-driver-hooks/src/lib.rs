//! The VM driver as a source of observability events.
//!
//! Implements the VM driver's hooks over the node's emitter: each hook call
//! becomes one `vm_driver` event, summarized — names, classifications and
//! sizes, never the values themselves. One [`Hooks`] value observes one
//! VM driver run of one VM; the wiring constructs it per VM with the VM's
//! identity, which the VM driver itself never knows.
//!
//! The hooks are generic over the summarizer of the run's effects, the
//! run's value and its VM driver error; an interpreter's effects reach the
//! payload through a [`SummarizeEffect`] type for them.
//!
//! [`SummarizeEffect`]: waymark_observability_events_vm_driver_hooks_core::SummarizeEffect

#![warn(missing_docs)]

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// The node's emitter, over the production payload: what the hooks emit
/// through, shared by every producer on the node.
pub type Emitter = waymark_observability_events_emitter::Emitter<
    waymark_ids::NodeId,
    waymark_observability_events_payload::Payload,
>;

/// Which of the run's optional observations the hooks record as events.
/// Every other observation is always recorded; the ones here are volume
/// without a consumer unless someone asks.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// Record a `snapshot_persisted` event per persisted snapshot — about
    /// one event in two of a run.
    pub snapshot_persisted: bool,
}

/// The VM driver hooks of one VM's run, emitting one event per hook call
/// it records.
///
/// Generic over the summarizer of the run's effects, the run's value and
/// its VM driver error, which it only ever summarizes.
pub struct Hooks<EffectSummarizer, Value, DriverError> {
    vm_id: waymark_ids::InstanceId,
    run_sequence: AtomicU64,
    emitter: Arc<Emitter>,
    policy: Policy,
    parameters: PhantomData<Parameters<EffectSummarizer, Value, DriverError>>,
}

/// The hooks' type parameters, held as a function pointer type so that
/// [`Hooks`] stays `Send`, `Sync` and `'static` whatever they are.
type Parameters<EffectSummarizer, Value, DriverError> =
    fn() -> (EffectSummarizer, Value, DriverError);

impl<EffectSummarizer, Value, DriverError> Hooks<EffectSummarizer, Value, DriverError> {
    /// The hooks for one run of `vm_id`, emitting through `emitter` what
    /// `policy` asks for; the run's positions start at zero.
    pub fn new(vm_id: waymark_ids::InstanceId, emitter: Arc<Emitter>, policy: Policy) -> Self {
        Self {
            vm_id,
            run_sequence: AtomicU64::new(0),
            emitter,
            policy,
            parameters: PhantomData,
        }
    }

    /// Emit `hook` as the run's next event.
    fn emit(&self, observation: waymark_observability_events_payload::vm_driver::Observation) {
        let run_sequence = self.run_sequence.fetch_add(1, Ordering::Relaxed);

        self.emitter
            .emit(waymark_observability_events_payload::Payload::VmDriver(
                waymark_observability_events_payload::vm_driver::Payload {
                    vm_id: self.vm_id,
                    run_sequence,
                    observation,
                },
            ));
    }
}

impl<EffectSummarizer, Value, DriverError> waymark_vm_driver_hooks::effect_emitted::HasEffect
    for Hooks<EffectSummarizer, Value, DriverError>
where
    EffectSummarizer: waymark_observability_events_vm_driver_hooks_core::SummarizeEffect,
{
    type Effect = EffectSummarizer::Effect;
}

impl<EffectSummarizer, Value, DriverError> waymark_vm_driver_hooks::promise_settled::HasValue
    for Hooks<EffectSummarizer, Value, DriverError>
{
    type Value = Value;
}

impl<
    EffectSummarizer,
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> waymark_vm_driver_hooks::vm_stopped::HasError
    for Hooks<
        EffectSummarizer,
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

impl<EffectSummarizer, Value, DriverError> waymark_vm_driver_hooks::VmStarted
    for Hooks<EffectSummarizer, Value, DriverError>
{
    fn vm_started(&self) {
        self.emit(waymark_observability_events_payload::vm_driver::Observation::VmStarted);
    }
}

impl<EffectSummarizer, Value, DriverError> waymark_vm_driver_hooks::EffectEmitted
    for Hooks<EffectSummarizer, Value, DriverError>
where
    EffectSummarizer: waymark_observability_events_vm_driver_hooks_core::SummarizeEffect<
            Summary = waymark_observability_events_payload::vm_driver::EffectSummary,
        >,
{
    fn effect_emitted(&self, number: EffectNumber, effect: &Self::Effect) {
        self.emit(
            waymark_observability_events_payload::vm_driver::Observation::EffectEmitted {
                effect_number: number,
                effect: EffectSummarizer::summarize_effect(effect),
            },
        );
    }
}

impl<EffectSummarizer, Value, DriverError> waymark_vm_driver_hooks::PromiseSettled
    for Hooks<EffectSummarizer, Value, DriverError>
{
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value>,
    ) {
        let settlement = match resolution {
            PromiseResolution::Resolved(_) => {
                waymark_observability_events_payload::vm_driver::Settlement::Resolved
            }
            PromiseResolution::Rejected(exception) => {
                waymark_observability_events_payload::vm_driver::Settlement::Rejected {
                    exception_type: exception.type_id.clone(),
                }
            }
        };

        self.emit(
            waymark_observability_events_payload::vm_driver::Observation::PromiseSettled {
                promise_state_id,
                settlement,
            },
        );
    }
}

impl<EffectSummarizer, Value, DriverError> waymark_vm_driver_hooks::SnapshotPersisted
    for Hooks<EffectSummarizer, Value, DriverError>
{
    fn snapshot_persisted(&self, size_in_bytes: usize) {
        if !self.policy.snapshot_persisted {
            return;
        }

        self.emit(
            waymark_observability_events_payload::vm_driver::Observation::SnapshotPersisted {
                size_in_bytes,
            },
        );
    }
}

impl<
    EffectSummarizer,
    Value,
    ExecutionError,
    SnapshotSerializationError,
    SnapshotPersistenceError,
    EffectHandlingError,
    GettingPromiseSettlementsError,
> waymark_vm_driver_hooks::VmStopped
    for Hooks<
        EffectSummarizer,
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
        let reason = match error {
            waymark_vm_driver::Error::Step(error) => {
                waymark_observability_events_payload::vm_driver::StopReason::Step {
                    error: format!("{error:?}"),
                }
            }
            waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises => {
                waymark_observability_events_payload::vm_driver::StopReason::NoReadyFramesOrWaitingPromises
            }
            waymark_vm_driver::Error::SnapshotSerialization(error) => {
                waymark_observability_events_payload::vm_driver::StopReason::SnapshotSerialization {
                    error: format!("{error:?}"),
                }
            }
            waymark_vm_driver::Error::SnapshotPersistence(error) => {
                waymark_observability_events_payload::vm_driver::StopReason::SnapshotPersistence {
                    error: format!("{error:?}"),
                }
            }
            waymark_vm_driver::Error::EffectHandling(error) => {
                waymark_observability_events_payload::vm_driver::StopReason::EffectHandling {
                    error: format!("{error:?}"),
                }
            }
            waymark_vm_driver::Error::GettingPromiseSettlements(error) => {
                waymark_observability_events_payload::vm_driver::StopReason::GettingPromiseSettlements {
                    error: format!("{error:?}"),
                }
            }
            waymark_vm_driver::Error::Cancelled => {
                waymark_observability_events_payload::vm_driver::StopReason::Cancelled
            }
        };

        self.emit(
            waymark_observability_events_payload::vm_driver::Observation::VmStopped { reason },
        );
    }
}

#[cfg(test)]
mod tests;
