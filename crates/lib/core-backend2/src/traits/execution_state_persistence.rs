use nonempty_collections::{IntoNonEmptyIterator, NEVec};
use serde::{Deserialize, Serialize};
use waymark_ids::InstanceId;
use waymark_runner_execution_core::ExecutionGraph;

use crate::{InstanceLockStatus, LockClaim};

/// Execution state persistence interface.
pub trait ExecutionStatePersistence {
    /// Error returned when persisting execution graph snapshots fails.
    type Error;

    /// Persist updated execution graphs.
    fn save_graphs<'a>(
        &'a self,
        claim: LockClaim,
        graph_updates: impl IntoNonEmptyIterator<Item = GraphUpdate> + 'a,
    ) -> impl Future<Output = Result<NEVec<InstanceLockStatus>, Self::Error>> + Send + 'a;
}

/// Batch payload representing an updated execution graph snapshot.
///
/// This intentionally stores only runtime nodes and edges (no DAG template or
/// derived caches) so persistence stays lightweight.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphUpdate {
    /// Instance whose execution graph snapshot is being updated.
    pub instance_id: InstanceId,

    /// Runtime execution graph snapshot to persist for the instance.
    #[serde(flatten)]
    pub graph: ExecutionGraph,
}
