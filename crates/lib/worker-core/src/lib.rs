//! Worker pool interface for executing actions.
//!
//! The pool's three capabilities are three traits, so a consumer names
//! only what it uses: a requester queues, a completions provider polls,
//! and a bringup launches.

use nonempty_collections::NEVec;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct WorkerPoolError {
    pub kind: String,
    pub message: String,
}

impl WorkerPoolError {
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
        }
    }
}

/// The pool can no longer serve: it has shut down and will answer
/// nothing further.
///
/// This is what the polling side used to say with a bare `None`.
#[derive(Debug, thiserror::Error)]
#[error("worker pool gone")]
pub struct WorkerPoolGoneError;

/// Start whatever background work a pool needs before it can serve.
pub trait LaunchWorkerPool {
    /// The error launching produces.
    type Error;

    /// Start any background tasks required by the pool.
    ///
    /// Default implementation is a no-op for pools that don't need launch work.
    fn launch(&self) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}

/// Submit action dispatches for execution.
pub trait QueueActionDispatch {
    /// The error queueing produces.
    type Error;

    /// Submit an action dispatch for execution.
    ///
    /// The dispatch is the protocol's own message: a pool speaks the
    /// worker protocol rather than a vocabulary of its own.  Correlation
    /// back to the caller rides on `metadata`, echoed verbatim onto the
    /// result.
    fn queue(
        &self,
        dispatch: waymark_proto::messages::ActionDispatch,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}

/// A report of one execution of a dispatched action, as the pool saw
/// it end.
///
/// Every dispatched action ends exactly one way per attempt: the worker
/// reports how the call completed, or the execution is lost — no result
/// will ever come from this attempt.  The pool reports the fact and
/// decides nothing: what a lost execution means for the awaiting
/// promise is the VM's business.
///
#[derive(Debug)]
pub enum ActionExecutionReport {
    /// The call completed: the worker reported how in the result
    /// payload.
    Completed(waymark_proto::messages::ActionResult),

    /// The execution was lost: no result will ever come from it.
    Lost(ActionExecutionLoss),
}

/// A lost execution, as the worker pool witnessed it.
#[derive(Debug)]
pub struct ActionExecutionLoss {
    /// The dispatch's metadata, echoed so the loss still routes back to
    /// the promise awaiting the call.
    pub metadata: Vec<u8>,

    /// How far the execution provably got.
    pub progress: ExecutionProgress,
}

/// How far a lost execution provably got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionProgress {
    /// The dispatch never reached the worker: the action did not start.
    NotStarted,

    /// The worker had the dispatch; how far it got is unknowable — the
    /// action may not have run at all, or may have run to completion
    /// with only the result lost.
    Unknown,
}

/// Await the results of dispatched actions.
pub trait PollActionResults {
    /// The error polling produces.
    ///
    /// This is the pool failing to serve at all — a pool that has shut
    /// down and will answer nothing further.  It is not how a single
    /// dispatch fails.
    type Error;

    /// Await and return a batch of action results, guaranteeing at least
    /// one action has completed.
    ///
    /// Results are [`proto::ActionResult`]s — how the call completed is
    /// the payload's own business, and correlation back to the caller
    /// rides solely on `metadata`, echoed verbatim from the dispatch.
    ///
    /// [`proto::ActionResult`]: waymark_proto::messages::ActionResult
    fn poll_complete(
        &self,
    ) -> impl Future<Output = Result<NEVec<ActionExecutionReport>, Self::Error>> + Send + '_;
}

impl<T> LaunchWorkerPool for std::sync::Arc<T>
where
    T: LaunchWorkerPool + Send + Sync,
{
    type Error = T::Error;

    async fn launch(&self) -> Result<(), Self::Error> {
        (**self).launch().await
    }
}

impl<T> QueueActionDispatch for std::sync::Arc<T>
where
    T: QueueActionDispatch + Send + Sync,
{
    type Error = T::Error;

    async fn queue(
        &self,
        dispatch: waymark_proto::messages::ActionDispatch,
    ) -> Result<(), Self::Error> {
        (**self).queue(dispatch).await
    }
}

impl<T> PollActionResults for std::sync::Arc<T>
where
    T: PollActionResults + Send + Sync,
{
    type Error = T::Error;

    fn poll_complete(
        &self,
    ) -> impl Future<Output = Result<NEVec<ActionExecutionReport>, Self::Error>> + Send + '_ {
        (**self).poll_complete()
    }
}

#[cfg(feature = "either")]
impl<Left, Right> LaunchWorkerPool for either::Either<Left, Right>
where
    Left: LaunchWorkerPool + Sync,
    Right: LaunchWorkerPool + Sync,
{
    type Error = either::Either<Left::Error, Right::Error>;

    async fn launch(&self) -> Result<(), Self::Error> {
        match self {
            either::Either::Left(left) => left.launch().await.map_err(either::Either::Left),
            either::Either::Right(right) => right.launch().await.map_err(either::Either::Right),
        }
    }
}

#[cfg(feature = "either")]
impl<Left, Right> QueueActionDispatch for either::Either<Left, Right>
where
    Left: QueueActionDispatch + Sync,
    Right: QueueActionDispatch + Sync,
{
    type Error = either::Either<Left::Error, Right::Error>;

    async fn queue(
        &self,
        dispatch: waymark_proto::messages::ActionDispatch,
    ) -> Result<(), Self::Error> {
        match self {
            either::Either::Left(left) => left.queue(dispatch).await.map_err(either::Either::Left),
            either::Either::Right(right) => {
                right.queue(dispatch).await.map_err(either::Either::Right)
            }
        }
    }
}

#[cfg(feature = "either")]
impl<Left, Right> PollActionResults for either::Either<Left, Right>
where
    Left: PollActionResults + Sync,
    Right: PollActionResults + Sync,
{
    type Error = either::Either<Left::Error, Right::Error>;

    async fn poll_complete(
        &self,
    ) -> Result<
        NEVec<ActionExecutionReport>,
        Self::Error,
    > {
        match self {
            either::Either::Left(left) => left.poll_complete().await.map_err(either::Either::Left),
            either::Either::Right(right) => {
                right.poll_complete().await.map_err(either::Either::Right)
            }
        }
    }
}
