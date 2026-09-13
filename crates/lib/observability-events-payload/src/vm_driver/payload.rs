use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

use super::{EffectKind, Kind, SettlementKind, StopKind};

/// One VM driver event.
///
/// The fields every observation shares sit beside it, so a VM's events are
/// reachable through one path however they were emitted. The observation
/// and its summaries are tagged internally by `kind`, so a reader addresses
/// their fields directly (`observation ->> 'kind'`,
/// `observation -> 'effect' ->> 'action_name'`).
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct Payload {
    /// The VM whose run emitted the event.
    #[schemars(schema_with = "uuid_schema")]
    pub vm_id: waymark_ids::InstanceId,

    /// The event's position within its driver run: 0 at the start of the
    /// run, one more per event of the same run. Runs restart the count, so
    /// a run's own completeness shows regardless of what its node dropped.
    pub run_sequence: u64,

    /// What was observed.
    pub observation: Observation,
}

impl Payload {
    /// The event's kind.
    pub fn kind(&self) -> Kind {
        self.observation.kind()
    }
}

/// What was observed of a run, one variant per observation, summarized.
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Observation {
    /// The driver run has started.
    VmStarted,

    /// The runtime has emitted an effect.
    EffectEmitted {
        /// The number the runtime assigned to the effect.
        #[schemars(with = "usize")]
        effect_number: EffectNumber,

        /// The effect, summarized.
        effect: EffectSummary,
    },

    /// A promise settlement has reached the driver.
    PromiseSettled {
        /// The settled promise.
        #[schemars(with = "usize")]
        promise_state_id: PromiseStateId,

        /// The settlement, summarized.
        settlement: Settlement,
    },

    /// A snapshot has been persisted.
    SnapshotPersisted {
        /// The size of the serialized snapshot.
        size_in_bytes: usize,
    },

    /// The driver run has stopped.
    VmStopped {
        /// Why, in the driver's own terms.
        reason: StopReason,
    },
}

impl Observation {
    /// The event's kind.
    pub fn kind(&self) -> Kind {
        match self {
            Observation::VmStarted => Kind::VmStarted,
            Observation::EffectEmitted { effect, .. } => Kind::EffectEmitted(effect.kind()),
            Observation::PromiseSettled { settlement, .. } => {
                Kind::PromiseSettled(settlement.kind())
            }
            Observation::SnapshotPersisted { .. } => Kind::SnapshotPersisted,
            Observation::VmStopped { reason } => Kind::VmStopped(reason.kind()),
        }
    }
}

/// An emitted effect, reduced to what identifies it.
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EffectSummary {
    /// The workflow completed with a value.
    Complete,

    /// The workflow raised an exception nothing caught.
    UnhandledException {
        /// The exception's type.
        exception_type: String,
    },

    /// An action was called.
    ActionCall {
        /// The promise the call settles.
        #[schemars(with = "usize")]
        promise_state_id: PromiseStateId,

        /// The called action.
        action_name: String,

        /// The module the action lives in, when the reference names one.
        module_name: Option<String>,
    },

    /// A sleep was requested.
    Sleep {
        /// The promise the sleep settles.
        #[schemars(with = "usize")]
        promise_state_id: PromiseStateId,

        /// How long.
        duration: core::time::Duration,

        /// Whether the sleep may be skipped.
        skip_allowed: bool,
    },
}

impl EffectSummary {
    /// The summary's kind.
    pub fn kind(&self) -> EffectKind {
        match self {
            EffectSummary::Complete => EffectKind::Complete,
            EffectSummary::UnhandledException { .. } => EffectKind::UnhandledException,
            EffectSummary::ActionCall { .. } => EffectKind::ActionCall,
            EffectSummary::Sleep { .. } => EffectKind::Sleep,
        }
    }
}

/// A promise settlement, reduced to its outcome.
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Settlement {
    /// Resolved with a value.
    Resolved,

    /// Rejected with an exception.
    Rejected {
        /// The exception's type.
        exception_type: String,
    },
}

impl Settlement {
    /// The settlement's kind.
    pub fn kind(&self) -> SettlementKind {
        match self {
            Settlement::Resolved => SettlementKind::Resolved,
            Settlement::Rejected { .. } => SettlementKind::Rejected,
        }
    }
}

/// Why a driver run stopped: the driver's own error, one variant each,
/// with the collaborator's error rendered where the driver carries one.
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StopReason {
    /// A VM step failed while executing an instruction.
    Step {
        /// The step error, rendered.
        error: String,
    },

    /// The runtime has no ready frames and no waiting promises.
    NoReadyFramesOrWaitingPromises,

    /// The snapshot serialization has failed.
    SnapshotSerialization {
        /// The serialization error, rendered.
        error: String,
    },

    /// The snapshot persistence has failed.
    SnapshotPersistence {
        /// The persistence error, rendered.
        error: String,
    },

    /// The effect handling has failed.
    EffectHandling {
        /// The handling error, rendered.
        error: String,
    },

    /// Getting promise settlements has failed.
    GettingPromiseSettlements {
        /// The settlements error, rendered.
        error: String,
    },

    /// The run was cancelled.
    Cancelled,
}

impl StopReason {
    /// The reason's kind.
    pub fn kind(&self) -> StopKind {
        match self {
            StopReason::Step { .. } => StopKind::Step,
            StopReason::NoReadyFramesOrWaitingPromises => StopKind::NoReadyFramesOrWaitingPromises,
            StopReason::SnapshotSerialization { .. } => StopKind::SnapshotSerialization,
            StopReason::SnapshotPersistence { .. } => StopKind::SnapshotPersistence,
            StopReason::EffectHandling { .. } => StopKind::EffectHandling,
            StopReason::GettingPromiseSettlements { .. } => StopKind::GettingPromiseSettlements,
            StopReason::Cancelled => StopKind::Cancelled,
        }
    }
}

/// The schema of an id serialized as its UUID's text: the id types carry
/// no schema of their own, and this crate describes the payload it
/// serializes.
fn uuid_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string",
        "format": "uuid",
    })
}
