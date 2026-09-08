//! Bringup for the essential-metrics subsystem: the pipeline from the
//! sampler through the lossy batcher into a store sink, plus the
//! retention sweep — over any backend implementing the essential-metrics
//! backend traits.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_essential_metrics_compat::BackendFlusher;
use waymark_essential_metrics_config::EssentialMetricsConfig;

/// The spawned essential-metrics tasks.
#[derive(Debug)]
pub struct Handles {
    /// The lossy batcher between the sampler and the store sink.
    pub batcher: tokio::task::JoinHandle<()>,

    /// The sampler.
    pub sampler: tokio::task::JoinHandle<()>,

    /// The retention sweep.
    pub retention: tokio::task::JoinHandle<()>,
}

/// Start the essential-metrics pipeline over `write_backend`: sampler →
/// lossy batcher → store sink, plus the retention sweep, all ending on
/// `shutdown_token` — and the essential-metrics API router over
/// `read_backend`.
///
/// `handle` is the sampling half of the recorder pair; the recording
/// half must already be installed in the process-global fanout, so the
/// metrics bound here (the batcher's own counters included) land in
/// live recorders.
pub fn start<WriteBackend, ReadBackend>(
    config: EssentialMetricsConfig,
    node_id: <WriteBackend as waymark_essential_metrics_sink_backend::HasNodeId>::NodeId,
    handle: waymark_essential_metrics_sampler::recorder::Handle,
    write_backend: Arc<WriteBackend>,
    read_backend: Arc<ReadBackend>,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> (Handles, aide::axum::ApiRouter)
where
    WriteBackend: waymark_essential_metrics_sink_backend::AppendSamples,
    WriteBackend: waymark_essential_metrics_retention_backend::ApplyRetention,
    WriteBackend: Send + Sync + 'static,
    <WriteBackend as waymark_essential_metrics_sink_backend::AppendSamples>::Error:
        std::fmt::Display,
    <WriteBackend as waymark_essential_metrics_sink_backend::HasNodeId>::NodeId:
        Clone + Send + Sync + 'static,
    ReadBackend: waymark_essential_metrics_query_backend::Latest,
    ReadBackend: waymark_essential_metrics_query_backend::Series,
    ReadBackend: waymark_essential_metrics_query_backend::HasNodeId<NodeId = waymark_ids::NodeId>,
    ReadBackend: Send + Sync + 'static,
{
    let api_router = waymark_api_essential_metrics_http::router(read_backend);

    let (batcher, batcher_task) = waymark_lossy_batcher::lossy_batcher(
        waymark_essential_metrics_sampler::bindings::BATCHER_NAME,
        config.lossy_batcher_policy,
        BackendFlusher(Arc::clone(&write_backend)),
        shutdown_token.clone().cancelled_owned(),
    );
    let sampler_task = waymark_essential_metrics_sampler::run(
        handle,
        node_id,
        config.sample_interval,
        batcher,
        shutdown_token.clone().cancelled_owned(),
    );
    let retention_task = waymark_retention_sweeper::run(
        "essential_metrics",
        config.retention,
        config.retention_sweep_interval,
        move |cutoff| {
            let write_backend = Arc::clone(&write_backend);
            async move { write_backend.apply_retention(cutoff).await }
        },
        shutdown_token.cancelled_owned(),
    );

    let handles = Handles {
        batcher: tokio::spawn(batcher_task),
        sampler: tokio::spawn(sampler_task),
        retention: tokio::spawn(retention_task),
    };

    (handles, api_router)
}
