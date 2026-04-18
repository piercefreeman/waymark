use derive_where::derive_where;
use waymark_ids::{InstanceId, WorkflowVersionId};

use crate::Timestamp;

/// The ability to enqueue workflow instances for execution.
pub trait InstanceEnqueue {
    /// Error returned when enqueuing instances fails.
    type Error;

    /// The metadata type that this backend supports for instances.
    type Metadata: waymark_core_backend_metadata::MetadataItems;

    /// Insert queued instances for run-loop consumption.
    fn enqueue_instances<'a>(
        &'a self,
        instances: impl Iterator<Item = EnqueueInstancesItem<Self::Metadata>> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}

/// Payload for a single workflow instance being inserted into the queue.
#[derive_where(Debug; waymark_core_backend_metadata::MetadataValues<Metadata>)]
pub struct EnqueueInstancesItem<Metadata: waymark_core_backend_metadata::MetadataItems> {
    /// Instance ID to enqueue the work with.
    /// Prepared externally and must be unique.
    pub id: InstanceId,

    /// The ID of the workflow version to use.
    ///
    /// The workflow version with this ID must exist when the instance is
    /// enqueued.
    pub workflow_version_id: WorkflowVersionId,

    /// The earliest time at which this instance will be picked up
    /// for execution.
    pub not_before: Timestamp,

    /// Backend-defined metadata to associate with the queued instance.
    pub metadata: waymark_core_backend_metadata::MetadataValues<Metadata>,
}
