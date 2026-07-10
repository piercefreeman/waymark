use nonempty_collections::NEVec;

/// The outcome of a completed action call — either a value or an exception.
pub enum ActionCallOutcome<Value> {
    /// The action completed successfully with this value.
    Value(Value),

    /// The action failed with this exception.
    Exception(waymark_vm_runtime_exception::Exception<Value>),
}

/// A completed action call, pairing its correlation metadata with the outcome.
pub struct ActionCallCompletion<Value, Metadata> {
    /// Correlation metadata identifying which call this completion is for.
    pub metadata: Metadata,

    /// The outcome of the action call.
    pub outcome: ActionCallOutcome<Value>,
}

/// The [`ActionCallCompletion`] type produced by a given
/// [`ActionCallCompletionsProvider`], with its value and metadata resolved.
pub type ActionCallCompletionFor<T> = ActionCallCompletion<
    <T as ActionCallCompletionsProvider>::Value,
    <T as ActionCallCompletionsProvider>::Metadata,
>;

/// A provider of completions for previously dispatched action calls.
///
/// Implementations check whether previously dispatched action calls have
/// finished and surface the results when they become available.
pub trait ActionCallCompletionsProvider {
    /// The type of a successful action result.
    type Value;

    /// The error returned when waiting for completions fails.
    type Error: core::fmt::Debug;

    /// The correlation metadata carried by each completion.
    type Metadata;

    /// Wait for action call completions to become available.
    ///
    /// Returns a non-empty list of [`ActionCallCompletion`]s when action calls
    /// have completed. Returns `Err(Self::Error)` if the wait itself failed
    /// (e.g., the provider has shut down).
    fn wait_for_completions(
        &mut self,
    ) -> impl Future<Output = Result<NEVec<ActionCallCompletionFor<Self>>, Self::Error>> + Send + '_;
}
