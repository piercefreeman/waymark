use nonempty_collections::NEVec;

/// The outcome of a completed action call — either a value or an exception.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ActionCallOutcome<Value> {
    /// The action completed successfully with this value.
    Value(Value),

    /// The action failed with this exception.
    Exception(waymark_vm_runtime_exception::Exception<Value>),
}

/// The stage an action call provably reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ActionCallStage {
    /// The call provably never started executing.
    NotStarted,

    /// Nothing is known about how far the call got.
    Unknown,
}

/// An action call was lost: the runtime can no longer learn how the call
/// completed — or whether it ever will — so there is no outcome to
/// report, only the stage the call provably reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActionCallLossError {
    /// The stage the call provably reached before it was lost.
    pub stage: ActionCallStage,
}

/// A completed action call, pairing its correlation metadata with how the
/// call ended.
pub struct ActionCallCompletion<Metadata, Value, ExecutionError> {
    /// Correlation metadata identifying which call this completion is for.
    pub metadata: Metadata,

    /// How the call ended: `Ok` carries the outcome the action produced,
    /// `Err` means the runtime failed to produce an outcome at all
    /// (e.g. the execution was lost).
    pub execution_result: Result<ActionCallOutcome<Value>, ExecutionError>,
}

/// The [`ActionCallCompletion`] type produced by a given
/// [`ActionCallCompletionsProvider`], with its value, execution error,
/// and metadata resolved.
pub type ActionCallCompletionFor<T> = ActionCallCompletion<
    <T as ActionCallCompletionsProvider>::Metadata,
    <T as ActionCallCompletionsProvider>::Value,
    <T as ActionCallCompletionsProvider>::ActionExecutionError,
>;

/// A provider of completions for previously dispatched action calls.
///
/// Implementations check whether previously dispatched action calls have
/// finished and surface the results when they become available.
pub trait ActionCallCompletionsProvider {
    /// The type of a successful action result.
    type Value;

    /// The error of a single call's execution failing to produce an
    /// outcome (e.g. a lost execution).
    ///
    /// Providers whose completions structurally always carry an outcome
    /// use [`core::convert::Infallible`].
    type ActionExecutionError;

    /// The error returned when waiting for completions fails.
    type WaitError: core::fmt::Debug;

    /// The correlation metadata carried by each completion.
    type Metadata;

    /// Wait for action call completions to become available.
    ///
    /// Returns a non-empty list of [`ActionCallCompletion`]s when action calls
    /// have completed. Returns `Err(Self::WaitError)` if the wait itself
    /// failed (e.g., the provider has shut down).
    fn wait_for_completions(
        &mut self,
    ) -> impl Future<Output = Result<NEVec<ActionCallCompletionFor<Self>>, Self::WaitError>> + Send + '_;
}
