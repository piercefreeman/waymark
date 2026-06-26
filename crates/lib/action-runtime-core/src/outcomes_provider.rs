use nonempty_collections::NEVec;

/// The outcome of a completed action call — either a value or an exception.
pub enum ActionCallOutcome<Value> {
    /// The action completed successfully with this value.
    Value(Value),

    /// The action failed with this exception.
    Exception(waymark_vm_runtime_exception::Exception<Value>),
}

/// A provider of outcomes for previously dispatched action calls.
///
/// Implementations check whether previously dispatched action calls have
/// finished and surface the results when they become available.
pub trait ActionCallOutcomesProvider {
    /// The type of a successful action result.
    type Value;

    /// The error returned when waiting for outcomes fails.
    type Error;

    /// Wait for action call outcomes to become available.
    ///
    /// Returns a non-empty list of [`ActionCallOutcome`]s when action calls
    /// have completed. Returns `Err(Self::Error)` if the wait itself failed
    /// (e.g., the provider has shut down).
    fn wait_for_outcomes(
        &mut self,
    ) -> impl Future<Output = Result<NEVec<ActionCallOutcome<Self::Value>>, Self::Error>> + Send + '_;
}
