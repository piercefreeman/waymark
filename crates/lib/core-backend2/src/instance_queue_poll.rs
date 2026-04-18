use std::{collections::HashMap, num::NonZeroUsize};

use chrono::{DateTime, Utc};
use nonempty_collections::NEVec;
use waymark_ids::{ExecutionId, InstanceId, ScheduleId, WorkflowVersionId};
use waymark_runner_execution_core::ExecutionGraph;
use waymark_runner_executor_core::UncheckedExecutionResult;

use crate::{LockClaim, QueuedInstance};

/// Polled instance.
#[derive(Debug)]
pub struct PolledInstance {
    /// The ID of the instance.
    pub id: InstanceId,

    /// The ID of the to workflow version of this instance.
    pub workflow_version_id: WorkflowVersionId,

    pub schedule_id: Option<ScheduleId>,
    pub entry_node: ExecutionId,
    pub graph: ExecutionGraph,
    #[serde(
        default = "HashMap::new",
        deserialize_with = "deserialize_action_results"
    )]
    pub action_results: HashMap<ExecutionId, UncheckedExecutionResult>,
    #[serde(default = "InstanceId::new_uuid_v4")]
    pub instance_id: InstanceId,
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

pub trait Error {
    /// Get the classification for the error kind.
    ///
    /// Can be used by the consumer to make decisions on how to handle an error
    /// without coupling to the implementation details.
    fn kind(&self) -> ErrorKind;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// An error is indicating there were no instances.
    NoInstances,

    /// An error indicaing some internal error condition.
    Internal,
}

impl Error for core::convert::Infallible {
    fn kind(&self) -> ErrorKind {
        // Infallible can never be constructed.
        match *self {}
    }
}
