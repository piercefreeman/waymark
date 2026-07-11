//! Error types for pending action call persistence.

use waymark_ids::InstanceId;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Error returned when storing a pending action call fails.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// The promise state id does not fit in a `BIGINT` column.
    #[error("promise state id {0:?} does not fit in a BIGINT")]
    PromiseStateIdOutOfRange(PromiseStateId),

    /// The effect number does not fit in a `BIGINT` column.
    #[error("effect number {0} does not fit in a BIGINT")]
    EffectNumberOutOfRange(waymark_vm_runtime_effect::EffectNumber),

    /// A diverging pending call was already stored under the same key.
    #[error(
        "conflicting pending action call already stored for vm {vm_id} promise {promise_state_id:?}"
    )]
    Conflict {
        /// The VM the pending call belongs to.
        vm_id: InstanceId,

        /// The promise the pending call fulfills.
        promise_state_id: PromiseStateId,
    },
}

/// Error returned when recording an action call outcome fails.
#[derive(Debug, thiserror::Error)]
pub enum StoreOutcomeError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// The promise state id does not fit in a `BIGINT` column.
    #[error("promise state id {0:?} does not fit in a BIGINT")]
    PromiseStateIdOutOfRange(PromiseStateId),
}

/// Error returned when removing a pending action call fails.
#[derive(Debug, thiserror::Error)]
pub enum RemoveError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// The promise state id does not fit in a `BIGINT` column.
    #[error("promise state id {0:?} does not fit in a BIGINT")]
    PromiseStateIdOutOfRange(PromiseStateId),
}

/// Error returned when loading the pending action calls fails.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The underlying database operation failed.
    #[error("sqlx: {0}")]
    Sqlx(#[source] sqlx::Error),

    /// A stored promise state id does not fit in a `usize` on this platform.
    #[error("stored promise state id {0} does not fit in a usize")]
    PromiseStateIdOutOfRange(i64),

    /// A stored effect number does not fit in a `usize` on this platform.
    #[error("stored effect number {0} does not fit in a usize")]
    EffectNumberOutOfRange(i64),

    /// The stored row carries both a result and an error.
    #[error(
        "corrupt pending action call row for vm {vm_id} promise {promise_state_id:?}: both result and error present"
    )]
    CorruptOutcome {
        /// The VM the pending call belongs to.
        vm_id: InstanceId,

        /// The promise the pending call fulfills.
        promise_state_id: PromiseStateId,
    },
}
