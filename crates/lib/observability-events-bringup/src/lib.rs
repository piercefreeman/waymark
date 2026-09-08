//! Bringup for the observability-events subsystem: the pipeline from the
//! emitter through the lossy batcher into a store sink, plus the
//! retention sweep — over any backend implementing the
//! observability-events backend traits — and the observability-events
//! API router over the same backend.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_observability_events_compat::BackendFlusher;
use waymark_observability_events_config::ObservabilityEventsConfig;

/// The label of the observability-events lossy batcher in its metrics.
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

/// Start the observability-events pipeline over `write_backend`: the
/// lossy batcher into the store sink, plus the retention sweep, all
/// ending on `shutdown_token` — and the observability-events API router
/// over `read_backend`, and the node's emitter for producers to share,
/// with what the VM driver hooks record through it.
///
/// The emitter is the node's one event stream: constructed here, once,
/// and shared behind an `Arc` by whoever produces events.
pub fn start<WriteBackend, ReadBackend>(
    config: ObservabilityEventsConfig,
    node_id: waymark_ids::NodeId,
    write_backend: Arc<WriteBackend>,
    read_backend: Arc<ReadBackend>,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> (
    Handles,
    aide::axum::ApiRouter,
    EmitterFor<WriteBackend>,
    waymark_observability_events_vm_driver_hooks::Policy,
)
where
    WriteBackend: waymark_observability_events_sink_backend::AppendEvents,
    WriteBackend:
        waymark_observability_events_sink_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    WriteBackend: waymark_observability_events_retention_backend::ApplyRetention,
    WriteBackend: Send + Sync + 'static,
    <WriteBackend as waymark_observability_events_sink_backend::AppendEvents>::Error:
        std::fmt::Display,
    <WriteBackend as waymark_observability_events_sink_backend::HasPayload>::Payload:
        Send + 'static,
    ReadBackend: waymark_observability_events_query_backend::ListEvents,
    ReadBackend: waymark_observability_events_query_backend::Tail,
    ReadBackend: waymark_observability_events_query_backend::VmTimeline,
    ReadBackend:
        waymark_observability_events_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    ReadBackend:
        waymark_observability_events_query_backend::HasVmId<VmId = waymark_ids::InstanceId>,
    ReadBackend: waymark_observability_events_query_backend::HasPayload,
    ReadBackend: Send + Sync + 'static,
    <ReadBackend as waymark_observability_events_query_backend::HasPayload>::Payload:
        waymark_observability_events_core::Kinded + serde::Serialize + schemars::JsonSchema,
{
    let api_router = waymark_api_observability_events_http::router(read_backend);

    let (batcher, batcher_task) = waymark_lossy_batcher::lossy_batcher(
        BATCHER_NAME,
        config.lossy_batcher_policy,
        BackendFlusher(Arc::clone(&write_backend)),
        shutdown_token.clone().cancelled_owned(),
    );
    let emitter = waymark_observability_events_emitter::Emitter::new(node_id, batcher);

    let retention_task = waymark_retention_sweeper::run(
        BATCHER_NAME,
        config.retention,
        config.retention_sweep_interval,
        move |cutoff| {
            let write_backend = Arc::clone(&write_backend);
            async move {
                waymark_observability_events_retention_backend::ApplyRetention::apply_retention(
                    &*write_backend,
                    cutoff,
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

    let vm_driver_hooks_policy = waymark_observability_events_vm_driver_hooks::Policy {
        snapshot_persisted: config.vm_driver_hooks_policy.record_snapshot_persisted,
    };

    (handles, api_router, emitter, vm_driver_hooks_policy)
}
