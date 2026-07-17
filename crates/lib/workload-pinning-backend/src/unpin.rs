//! Unpin workloads.

use nonempty_collections::IntoNonEmptyIterator;
use waymark_workload_pinning_core::UnpinMode;

use crate::{HasNodeId, HasWorkloadId};

/// The ability to unpin workloads, ending this node's pinnings on them.
///
/// The [`UnpinMode`] of each workload decides what happens to it next:
/// a released workload stays runnable and may be pinned again, a parked
/// workload leaves the runnable set and stays unpinnable until it is
/// made runnable again.
///
/// Workloads not currently pinned by `node_id` are silently skipped.
///
/// The behavior is unspecified when the same workload appears more than
/// once in the batch.
pub trait UnpinWorkloads: HasNodeId + HasWorkloadId {
    /// Error returned when unpinning workloads fails.
    type Error: std::fmt::Debug;

    /// Unpin workloads, each according to its mode.
    fn unpin_workloads<'a>(
        &'a self,
        node_id: Self::NodeId,
        workloads: impl IntoNonEmptyIterator<Item = (Self::WorkloadId, UnpinMode)> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
