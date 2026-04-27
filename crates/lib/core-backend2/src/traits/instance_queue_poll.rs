use std::{collections::HashMap, num::NonZeroUsize};

use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use waymark_ids::{ExecutionId, InstanceId, WorkflowVersionId};
use waymark_runner_execution_core::ExecutionGraph;
use waymark_runner_executor_core::UncheckedExecutionResult;

use crate::LockClaim;

/// Polled instance.
#[derive(Debug)]
pub struct PolledInstance {
    /// The ID of the instance.
    pub id: InstanceId,

    /// The workflow version this queued instance should execute.
    pub workflow_version_id: WorkflowVersionId,

    /// Execution graph snapshot for the claimed instance.
    ///
    /// Rehydrate before execution.
    pub graph: ExecutionGraph,

    /// Previously persisted action results needed to resume execution.
    pub action_results: HashMap<ExecutionId, UncheckedExecutionResult>,
}

/// The ability to poll workflow instance queue for new work.
pub trait InstanceQueuePoll {
    /// An error that can occur while polling the queued instances.
    type Error: Error;

    /// Return up to size queued instances without blocking.
    fn poll_queued_instances(
        &self,
        now: DateTime<Utc>,
        claim: LockClaim,
        max_instances: NonZeroUsize,
    ) -> impl Future<Output = Result<NEVec<PolledInstance>, Self::Error>> + Send + '_;
}

/// Classification interface for queue polling errors.
pub trait Error {
    /// Get the classification for the error kind.
    ///
    /// Can be used by the consumer to make decisions on how to handle an error
    /// without coupling to the implementation details.
    fn kind(&self) -> ErrorKind;
}

/// Stable categories for queue polling failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// An error is indicating there were no instances.
    NoInstances,

    /// An error indicating some internal backend failure condition.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        // Infallible can never be constructed.
        match *self {}
    }
}
