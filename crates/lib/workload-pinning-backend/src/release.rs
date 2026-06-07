//! Release pinnings from instances.

use nonempty_collections::IntoNonEmptyIterator;

use crate::{HasInstanceId, HasNodeId};

/// The ability to release held pinnings on workload queue entries.
pub trait ReleasePinnings: HasNodeId + HasInstanceId {
    /// Error returned when releasing pinning fails.
    type Error;

    /// Release instance pinnings when evicting workloads.
    fn release_pinnings<'a>(
        &'a self,
        node_id: Self::NodeId,
        instance_ids: impl IntoNonEmptyIterator<Item = Self::InstanceId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
