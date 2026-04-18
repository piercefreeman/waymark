use chrono::{DateTime, Utc};
use waymark_ids::{InstanceId, WorkflowVersionId};

#[derive(Debug)]
pub struct EnqueueInstance<Metadata> {
    /// Instance ID to enqueue the work with.
    /// Prepared externally and must be unique.
    pub id: InstanceId,

    /// The ID of the workflow version to use.
    ///
    /// The workflow version with this ID must exist when the is enqueued.
    pub workflow_version_id: WorkflowVersionId,

    /// The earliest time at which this instance will be picked up
    /// for execution.
    pub not_before: DateTime<Utc>,

    /// The instance metadata type.
    pub metadata: Metadata,
}

/// The ability to enqueue workflow instances for execution.
pub trait InstanceEnqueue {
    /// An error that can occur while enqueuing instances.
    type Error;

    /// The metadata type that this backend supports for instances.
    type Metadata;

    /// Insert queued instances for run-loop consumption.
    fn enqueue_instances<'a>(
        &'a self,
        instances: impl Iterator<Item = EnqueueInstance<Self::Metadata>> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
