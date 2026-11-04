//! A completions provider aggregating two completions providers.

#![warn(missing_docs)]

#[cfg(test)]
mod tests;

use nonempty_collections::NEVec;
use waymark_action_runtime_core::ActionCallCompletion;

/// A completions provider aggregating two: a batch from whichever side
/// has one, and `None` once both sides have shut down.
///
/// Each wait races the two sides and drops the wait of the side that did
/// not win, so both sides' waits must be safe to drop before they
/// complete.
pub struct AggregateActionCallCompletionsProvider<First, Second> {
    /// The first side; `None` once it has shut down.
    pub first: Option<First>,

    /// The second side; `None` once it has shut down.
    pub second: Option<Second>,
}

impl<First, Second> AggregateActionCallCompletionsProvider<First, Second> {
    /// The aggregate of `first` and `second`.
    pub fn new(first: First, second: Second) -> Self {
        Self {
            first: Some(first),
            second: Some(second),
        }
    }
}

/// Error returned when one side's wait fails.
#[derive(Debug, thiserror::Error)]
pub enum WaitError<First, Second> {
    /// The first side's wait failed.
    #[error("waiting for the first side's completions: {0}")]
    First(#[source] First),

    /// The second side's wait failed.
    #[error("waiting for the second side's completions: {0}")]
    Second(#[source] Second),
}

/// The side a round of waiting answered from.
enum Side {
    First,
    Second,
}

impl<First, Second> waymark_action_runtime_core::ActionCallCompletionsProvider
    for AggregateActionCallCompletionsProvider<First, Second>
where
    First: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Second: waymark_action_runtime_core::ActionCallCompletionsProvider<
            Value = First::Value,
            RaisedException = First::RaisedException,
            ActionExecutionError = First::ActionExecutionError,
            Metadata = First::Metadata,
        >,
    First: Send,
    Second: Send,
{
    type Value = First::Value;
    type RaisedException = First::RaisedException;
    type ActionExecutionError = First::ActionExecutionError;
    type WaitError = WaitError<First::WaitError, Second::WaitError>;
    type Metadata = First::Metadata;

    async fn wait_for_completions(
        &mut self,
    ) -> Result<
        Option<
            NEVec<
                ActionCallCompletion<
                    First::Metadata,
                    First::Value,
                    First::RaisedException,
                    First::ActionExecutionError,
                >,
            >,
        >,
        Self::WaitError,
    > {
        loop {
            let (side, result) = match (self.first.as_mut(), self.second.as_mut()) {
                (None, None) => return Ok(None),
                (Some(first), None) => {
                    let result = first.wait_for_completions().await;

                    (Side::First, result.map_err(WaitError::First))
                }
                (None, Some(second)) => {
                    let result = second.wait_for_completions().await;

                    (Side::Second, result.map_err(WaitError::Second))
                }
                (Some(first), Some(second)) => {
                    let first_wait = std::pin::pin!(first.wait_for_completions());
                    let second_wait = std::pin::pin!(second.wait_for_completions());

                    match futures_util::future::select(first_wait, second_wait).await {
                        futures_util::future::Either::Left((result, _second_wait)) => {
                            (Side::First, result.map_err(WaitError::First))
                        }
                        futures_util::future::Either::Right((result, _first_wait)) => {
                            (Side::Second, result.map_err(WaitError::Second))
                        }
                    }
                }
            };

            match result? {
                Some(completions) => return Ok(Some(completions)),
                None => match side {
                    Side::First => self.first = None,
                    Side::Second => self.second = None,
                },
            }
        }
    }
}
