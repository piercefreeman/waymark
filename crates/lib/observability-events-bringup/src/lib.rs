//! Bringup for the observability-events family: the pipeline from the
//! emitter through the lossy batcher into a store sink, plus the
//! retention sweep — over any backend implementing the family's
//! backend traits — and the family's API router over the same backend.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_observability_events_compat::BackendFlusher;
use waymark_observability_events_config::ObservabilityEventsConfig;

/// The label of this family's lossy batcher in its metrics.
const BATCHER_NAME: &str = "observability_events";

/// The spawned observability-events tasks.
#[derive(Debug)]
pub struct Handles {
    /// The lossy batcher between the emitter and the store sink.
    pub batcher: tokio::task::JoinHandle<()>,

    /// The retention sweep.
    pub retention: tokio::task::JoinHandle<()>,
}

/// The emitter a bringup hands out: the node's one event stream, over
/// the backend's payload.
pub type EmitterFor<Backend> = waymark_observability_events_emitter::Emitter<
    waymark_ids::NodeId,
    <Backend as waymark_observability_events_sink_backend::HasPayload>::Payload,
>;

/// Start the observability-events pipeline over `backend`: the lossy
/// batcher into the store sink, plus the retention sweep, all ending on
/// `shutdown_token` — and the family's API router over the same
/// backend, and the node's emitter for producers to share.
///
/// The emitter is the node's one event stream: constructed here, once,
/// and shared behind an `Arc` by whoever produces events.
pub fn start<Backend>(
    config: ObservabilityEventsConfig,
    node_id: waymark_ids::NodeId,
    backend: Arc<Backend>,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> (Handles, aide::axum::ApiRouter, EmitterFor<Backend>)
where
    Backend: waymark_observability_events_sink_backend::AppendEvents,
    Backend: waymark_observability_events_sink_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_retention_backend::ApplyRetention,
    Backend: waymark_observability_events_query_backend::ListEvents,
    Backend: waymark_observability_events_query_backend::Tail,
    Backend: waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    Backend: waymark_observability_events_query_backend::HasPayload,
    Backend: Send + Sync + 'static,
    <Backend as waymark_observability_events_sink_backend::AppendEvents>::Error: std::fmt::Display,
    <Backend as waymark_observability_events_sink_backend::HasPayload>::Payload: Send + 'static,
    <Backend as waymark_observability_events_query_backend::HasPayload>::Payload:
        waymark_observability_events_core::EventKind + serde::Serialize,
    <<Backend as waymark_observability_events_query_backend::HasPayload>::Payload as waymark_observability_events_core::EventKind>::Kind:
        Into<&'static str>,
{
    let api_router = waymark_api_observability_events_http::router(Arc::clone(&backend));

    let (batcher, batcher_task) = waymark_lossy_batcher::lossy_batcher(
        BATCHER_NAME,
        config.lossy_batcher_policy,
        BackendFlusher(Arc::clone(&backend)),
        shutdown_token.clone().cancelled_owned(),
    );
    let emitter = waymark_observability_events_emitter::Emitter::new(node_id, batcher);

    let retention_task = waymark_retention_sweeper::run(
        BATCHER_NAME,
        config.retention,
        config.retention_sweep_interval,
        move |cutoff| {
            let backend = Arc::clone(&backend);
            async move {
                waymark_observability_events_retention_backend::ApplyRetention::apply_retention(
                    &*backend, cutoff,
                )
                .await
            }
        },
        shutdown_token.cancelled_owned(),
    );

    let handles = Handles {
        batcher: tokio::spawn(batcher_task),
        retention: tokio::spawn(retention_task),
    };

    (handles, api_router, emitter)
}
