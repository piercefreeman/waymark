//! A time-bucketed series of one node's samples.

use waymark_essential_metrics_core::NodeSample;

/// Parameters of a series read.
#[derive(Debug)]
pub struct Params<NodeId> {
    /// The node to read.
    pub node_id: NodeId,

    /// Inclusive start of the time range.
    pub from: chrono::DateTime<chrono::Utc>,

    /// Exclusive end of the time range.
    pub to: chrono::DateTime<chrono::Utc>,

    /// Bucket width; samples within one bucket are aggregated (gauges and
    /// medians averaged, cumulative counters and timestamps at their
    /// maximum).
    pub bucket: waymark_nonzero_duration::NonZeroDuration,
}

/// Read one node's samples over a time range, bucketed.
pub trait Series: crate::HasNodeId {
    /// Error type for the read.
    type Error: std::fmt::Debug;

    /// One aggregated sample per non-empty bucket, ascending by time;
    /// `sampled_at` carries the bucket start.
    fn series(
        &self,
        params: Params<Self::NodeId>,
    ) -> impl Future<Output = Result<Vec<NodeSample<Self::NodeId>>, Self::Error>> + Send + '_;
}
