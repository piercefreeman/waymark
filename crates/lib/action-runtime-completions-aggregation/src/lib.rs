//! A completions provider aggregating two completions providers.

#![warn(missing_docs)]

#[cfg(test)]
mod tests;

use std::pin::Pin;

use nonempty_collections::NEVec;
use waymark_action_runtime_core::{ActionCallCompletion, ActionCallCompletionFor};

/// What a side's wait resolves to: the provider back, and what its wait
/// returned.
pub type Waited<Provider> = (
    Provider,
    Result<
        Option<NEVec<ActionCallCompletionFor<Provider>>>,
        <Provider as waymark_action_runtime_core::ActionCallCompletionsProvider>::WaitError,
    >,
);

/// A side's wait in flight: it owns the provider and hands it back when
/// it resolves.
pub type Wait<Provider> = Pin<Box<dyn Future<Output = Waited<Provider>> + Send>>;

/// One side of an [`AggregateActionCallCompletionsProvider`].
pub enum Side<Provider>
where
    Provider: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    /// The provider, with no wait in flight.
    Idle(Provider),

    /// A wait in flight; it is kept until it resolves, whichever side a
    /// round goes to.
    Waiting(Wait<Provider>),

    /// The provider has shut down.
    ShutDown,
}

impl<Provider> Side<Provider>
where
    Provider: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Provider: Send + 'static,
{
    /// Start a wait on an idle provider.
    fn arm(&mut self) {
        let taken = std::mem::replace(self, Side::ShutDown);

        *self = match taken {
            Side::Idle(mut provider) => Side::Waiting(Box::pin(async move {
                let result = provider.wait_for_completions().await;

                (provider, result)
            })),
            waiting_or_shut_down => waiting_or_shut_down,
        };
    }

    /// The wait in flight, if any.
    fn wait(&mut self) -> Option<&mut Wait<Provider>> {
        match self {
            Side::Waiting(wait) => Some(wait),
            Side::Idle(_) | Side::ShutDown => None,
        }
    }

    /// Take the provider back from what its wait resolved to, shutting the
    /// side down if the provider did, and hand out the wait's result.
    fn settle(
        &mut self,
        waited: Waited<Provider>,
    ) -> Result<Option<NEVec<ActionCallCompletionFor<Provider>>>, Provider::WaitError> {
        let (provider, result) = waited;

        *self = match &result {
            Ok(None) => Side::ShutDown,
            Ok(Some(_)) | Err(_) => Side::Idle(provider),
        };

        result
    }
}

/// The side a round looks at first when both sides are waiting.
#[derive(Debug)]
pub enum Preferred {
    /// The first side.
    First,

    /// The second side.
    Second,
}

/// A completions provider aggregating two: a batch from whichever side
/// has one, and `None` once both sides have shut down.
///
/// A side's wait, once begun, is kept until it resolves: a round the
/// other side wins leaves it in flight for the next call, so nothing a
/// wait took is lost.  The side that lost a round is looked at first in
/// the next one, so a side that is always ready cannot starve the other.
pub struct AggregateActionCallCompletionsProvider<First, Second>
where
    First: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Second: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    /// The first side.
    pub first: Side<First>,

    /// The second side.
    pub second: Side<Second>,

    /// The side the next round looks at first.
    pub preferred: Preferred,
}

impl<First, Second> AggregateActionCallCompletionsProvider<First, Second>
where
    First: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Second: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    /// The aggregate of `first` and `second`.
    pub fn new(first: First, second: Second) -> Self {
        Self {
            first: Side::Idle(first),
            second: Side::Idle(second),
            preferred: Preferred::First,
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

/// Which side a round of waiting resolved from, with what it resolved to.
enum Round<First, Second>
where
    First: waymark_action_runtime_core::ActionCallCompletionsProvider,
    Second: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    First(Waited<First>),
    Second(Waited<Second>),
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
    First: Send + 'static,
    Second: Send + 'static,
    First::Value: Send,
    First::RaisedException: Send,
    First::ActionExecutionError: Send,
    First::Metadata: Send,
    First::WaitError: Send,
    Second::WaitError: Send,
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
            self.first.arm();
            self.second.arm();

            let round = match (self.first.wait(), self.second.wait()) {
                (None, None) => return Ok(None),
                (Some(first), None) => Round::First(first.await),
                (None, Some(second)) => Round::Second(second.await),
                (Some(first), Some(second)) => match self.preferred {
                    Preferred::First => match futures_util::future::select(first, second).await {
                        futures_util::future::Either::Left((waited, _second)) => {
                            Round::First(waited)
                        }
                        futures_util::future::Either::Right((waited, _first)) => {
                            Round::Second(waited)
                        }
                    },
                    Preferred::Second => match futures_util::future::select(second, first).await {
                        futures_util::future::Either::Left((waited, _first)) => {
                            Round::Second(waited)
                        }
                        futures_util::future::Either::Right((waited, _second)) => {
                            Round::First(waited)
                        }
                    },
                },
            };

            let result = match round {
                Round::First(waited) => {
                    self.preferred = Preferred::Second;

                    self.first.settle(waited).map_err(WaitError::First)
                }
                Round::Second(waited) => {
                    self.preferred = Preferred::First;

                    self.second.settle(waited).map_err(WaitError::Second)
                }
            };

            if let Some(completions) = result? {
                return Ok(Some(completions));
            }
        }
    }
}
