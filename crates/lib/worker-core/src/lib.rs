//! Worker pool interface for executing actions.

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

/// Abstract worker pool with queue and batch completion polling.
pub trait BaseWorkerPool {
    /// Start any background tasks required by the pool.
    ///
    /// Default implementation is a no-op for pools that don't need launch work.
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + Send + '_ {
        async { Ok(()) }
    }

    /// Submit an action dispatch for execution.
    ///
    /// The dispatch is the protocol's own message: a pool speaks the
    /// worker protocol rather than a vocabulary of its own.  Correlation
    /// back to the caller rides on `metadata`, echoed verbatim onto the
    /// completion.
    fn queue(
        &self,
        dispatch: waymark_proto::messages::ActionDispatch,
    ) -> Result<(), WorkerPoolError>;

    /// Await and return a batch of completed actions, guaranteeing at least
    /// one action has completed.
    ///
    /// Completions are [`proto::ActionResult`]s — success/failure is
    /// discriminated structurally by the message's own fields, and
    /// correlation back to the caller rides solely on `metadata`,
    /// echoed verbatim from the request.
    ///
    /// [`proto::ActionResult`]: waymark_proto::messages::ActionResult
    fn poll_complete(
        &self,
    ) -> impl Future<Output = Option<NEVec<waymark_proto::messages::ActionResult>>> + Send + '_;
}

impl<T> BaseWorkerPool for std::sync::Arc<T>
where
    T: BaseWorkerPool + Send + Sync,
{
    async fn launch(&self) -> Result<(), WorkerPoolError> {
        (**self).launch().await
    }

    fn queue(
        &self,
        dispatch: waymark_proto::messages::ActionDispatch,
    ) -> Result<(), WorkerPoolError> {
        (**self).queue(dispatch)
    }

    async fn poll_complete(&self) -> Option<NEVec<waymark_proto::messages::ActionResult>> {
        (**self).poll_complete().await
    }
}

#[cfg(feature = "either")]
impl<Left: BaseWorkerPool, Right: BaseWorkerPool> BaseWorkerPool for either::Either<Left, Right> {
    fn launch(&self) -> impl Future<Output = Result<(), WorkerPoolError>> + Send + '_ {
        match self {
            either::Either::Left(left) => either::Either::Left(left.launch()),
            either::Either::Right(right) => either::Either::Right(right.launch()),
        }
    }

    fn queue(
        &self,
        dispatch: waymark_proto::messages::ActionDispatch,
    ) -> Result<(), WorkerPoolError> {
        match self {
            either::Either::Left(left) => left.queue(dispatch),
            either::Either::Right(right) => right.queue(dispatch),
        }
    }

    fn poll_complete(
        &self,
    ) -> impl Future<Output = Option<NEVec<waymark_proto::messages::ActionResult>>> + Send + '_
    {
        match self {
            either::Either::Left(left) => either::Either::Left(left.poll_complete()),
            either::Either::Right(right) => either::Either::Right(right.poll_complete()),
        }
    }
}
