//! Backend trait for the essential-metrics sink: appending samples to a
//! store.

#![warn(missing_docs)]

use nonempty_collections::NESlice;
use waymark_essential_metrics_core::NodeSample;

/// The node identity type of a backend's samples.
pub trait HasNodeId {
    /// The node identity type.
    type NodeId;
}

/// Append a batch of samples to the store.
pub trait AppendSamples: HasNodeId {
    /// Error type for append operations.
    type Error: std::fmt::Debug;

    /// Append the given samples in one batch.
    fn append_samples<'a>(
        &'a self,
        samples: NESlice<'a, NodeSample<Self::NodeId>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
