use derive_where::derive_where;
use nonempty_collections::IntoNonEmptyIterator;
use serde::{Deserialize, Serialize};
use waymark_ids::ExecutionId;
use waymark_runner_executor_core::UncheckedExecutionResult;

/// Execution state persistence interface.
pub trait ActionsDonePersistence {
    /// Error returned when persisting completed action attempts fails.
    type Error;

    /// The metadata type that this backend supports for action completions.
    type Metadata: waymark_core_backend_metadata::MetadataItems;

    /// Persist finished action attempts (success or failure).
    fn save_actions_done<'a>(
        &'a self,
        actions: impl IntoNonEmptyIterator<Item = ActionDone<Self::Metadata>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Batch payload representing a finished action attempt (success or failure).
#[derive_where(Debug; waymark_core_backend_metadata::MetadataValues<Metadata>)]
pub struct ActionDone<Metadata: waymark_core_backend_metadata::MetadataItems> {
    /// Execution node whose action attempt finished.
    pub execution_id: ExecutionId,

    /// Attempt number that produced this terminal result.
    pub attempt: u32,

    /// Terminal status for the attempt.
    pub status: ActionAttemptStatus,

    /// Serialized action result payload, including failures when applicable.
    pub result: UncheckedExecutionResult,

    /// Backend-defined metadata to associate with the action completion.
    pub metadata: waymark_core_backend_metadata::MetadataValues<Metadata>,
}

/// Terminal outcome for a persisted action attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionAttemptStatus {
    /// The action produced a successful result.
    Completed,

    /// The action finished with an explicit failure result.
    Failed,

    /// The attempt exceeded its execution deadline.
    TimedOut,
}

impl std::fmt::Display for ActionAttemptStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::TimedOut => write!(f, "timed_out"),
        }
    }
}
