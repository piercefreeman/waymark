//! Release pinnings from workloads.

use nonempty_collections::IntoNonEmptyIterator;

use crate::{HasNodeId, HasWorkloadId};

/// The ability to release held pinnings on workloads.
pub trait ReleasePinnings: HasNodeId + HasWorkloadId {
    /// Error returned when releasing pinning fails.
    type Error: std::fmt::Debug;

    /// Release workload pinnings when evicting workloads.
    fn release_pinnings<'a>(
        &'a self,
        node_id: Self::NodeId,
        workload_ids: impl IntoNonEmptyIterator<Item = Self::WorkloadId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
