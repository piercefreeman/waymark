use nonempty_collections::NEVec;

use crate::WithActionCallMetadata;

/// The outcome of a completed action call — either a value or an exception.
pub enum ActionCallOutcome<Value> {
    /// The action completed successfully with this value.
    Value(Value),

    /// The action failed with this exception.
    Exception(waymark_vm_runtime_exception::Exception<Value>),
}

/// A completed action call, pairing its originating effect and promise
/// with the outcome.
pub struct ActionCallCompletion<Value, Metadata> {
    /// The outcome of the action call.
    pub outcome: ActionCallOutcome<Value>,

    /// Metadata about the action call.
    pub metadata: Metadata,
}

/// A convenience alias for [`ActionCallCompletion`] that infers the `Value` and
/// `Metadata` type parameters from a provider that implements
/// [`ActionCallCompletionsProvider`].
pub type ActionCallCompletionFor<Provider> = ActionCallCompletion<
    <Provider as self::ActionCallCompletionsProvider>::Value,
    crate::ActionCallMetadataFor<Provider>,
>;

/// A provider of completions for previously dispatched action calls.
///
/// Implementations check whether previously dispatched action calls have
/// finished and surface the results when they become available.
pub trait ActionCallCompletionsProvider: WithActionCallMetadata {
    /// The type of a successful action result.
    type Value;

    /// The error returned when waiting for completions fails.
    type Error;

    /// Wait for action call completions to become available.
    ///
    /// Returns a non-empty list of [`ActionCallCompletion`]s when action calls
    /// have completed. Returns `Err(Self::Error)` if the wait itself failed
    /// (e.g., the provider has shut down).
    fn wait_for_completions(
        &mut self,
    ) -> impl Future<Output = Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error>> + Send + '_;
}
