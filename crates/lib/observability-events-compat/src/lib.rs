//! Compatibility between the observability-events backend traits and
//! the machinery around them.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_observability_events_sink_backend::EventFor;

/// A backend's [`AppendEvents`] as a lossy batcher flusher.
///
/// [`AppendEvents`]: waymark_observability_events_sink_backend::AppendEvents
pub struct BackendFlusher<Backend>(pub Arc<Backend>);

impl<Backend> waymark_lossy_batcher::Flusher<EventFor<Backend>> for BackendFlusher<Backend>
where
    Backend: waymark_observability_events_sink_backend::AppendEvents,
    Backend::Error: std::fmt::Display,
{
    type Error = Backend::Error;

    fn flush<'a>(
        &'a self,
        batch: nonempty_collections::NESlice<'a, EventFor<Backend>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.0.append_events(batch)
    }
}
