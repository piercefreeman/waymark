//! The latest sample of every node.

use waymark_essential_metrics_core::NodeSample;

/// Read every node's most recent sample.
pub trait Latest: crate::HasNodeId {
    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// The most recent sample per node, in unspecified node order.
    fn latest(
        &self,
    ) -> impl Future<Output = Result<Vec<NodeSample<Self::NodeId>>, Self::Error>> + Send + '_;
}
