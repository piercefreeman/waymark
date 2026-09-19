//! Compatibility between the essential-metrics backend traits and the
//! machinery around them.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_essential_metrics_core::NodeSample;

/// A backend's [`AppendSamples`] as a lossy batcher flusher.
pub struct BackendFlusher<Backend>(pub Arc<Backend>);

impl<Backend> waymark_lossy_batcher::Flusher<NodeSample<Backend::NodeId>>
    for BackendFlusher<Backend>
where
    Backend: waymark_essential_metrics_sink_backend::AppendSamples,
    Backend::Error: std::fmt::Display,
{
    type Error = Backend::Error;

    fn flush<'a>(
        &'a self,
        batch: nonempty_collections::NESlice<'a, NodeSample<Backend::NodeId>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.0.append_samples(batch)
    }
}
