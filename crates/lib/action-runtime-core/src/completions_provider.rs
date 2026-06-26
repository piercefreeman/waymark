use nonempty_collections::NEVec;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// The outcome of a completed action call — either a value or an exception.
pub enum ActionCallOutcome<Value> {
    /// The action completed successfully with this value.
    Value(Value),

    /// The action failed with this exception.
    Exception(waymark_vm_runtime_exception::Exception<Value>),
}

/// A completed action call, pairing its originating effect and promise
/// with the outcome.
pub struct ActionCallCompletion<Value> {
    /// The sequential number of the effect that triggered this call.
    pub effect_number: EffectNumber,

    /// The id of a promise state this action completion is for.
    pub promise_state_id: PromiseStateId,

    /// The outcome of the action call.
    pub outcome: ActionCallOutcome<Value>,
}

/// A provider of completions for previously dispatched action calls.
///
/// Implementations check whether previously dispatched action calls have
/// finished and surface the results when they become available.
pub trait ActionCallCompletionsProvider {
    /// The type of a successful action result.
    type Value;

    /// The error returned when waiting for completions fails.
    type Error: core::fmt::Debug;

    /// Wait for action call completions to become available.
    ///
    /// Returns a non-empty list of [`ActionCallCompletion`]s when action calls
    /// have completed. Returns `Err(Self::Error)` if the wait itself failed
    /// (e.g., the provider has shut down).
    fn wait_for_completions(
        &mut self,
    ) -> impl Future<Output = Result<NEVec<ActionCallCompletion<Self::Value>>, Self::Error>> + Send + '_;
}
