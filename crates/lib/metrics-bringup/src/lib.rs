//! Bringup for the process-global metrics recorder: the essential-metrics
//! recorder and the Prometheus recorder composed into one fanout,
//! installed once — the only `set_global_recorder` call in the system.
//!
//! Only registration happens here: the returned handles are the two
//! recorders' counterparts, for the observability bringup to run the
//! essential-metrics pipeline over, and for the Prometheus endpoint and
//! upkeep to serve.

#![warn(missing_docs)]

/// Error returned by [`register`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    /// The Prometheus exporter could not be built.
    #[error("building the prometheus exporter: {0}")]
    Build(#[source] metrics_exporter_prometheus::BuildError),

    /// A process-global metrics recorder is already installed.
    #[error("a process-global metrics recorder is already installed")]
    AlreadyInstalled,
}

/// Install the process-global metrics recorder — a fanout of the
/// essential-metrics recorder and the Prometheus recorder. Metrics bound
/// after this call land in both.
///
/// Returns the essential-metrics sampling handle and the Prometheus
/// handle: the recorders' read-side counterparts.
pub fn register() -> Result<
    (
        waymark_essential_metrics_sampler::recorder::Handle,
        metrics_exporter_prometheus::PrometheusHandle,
    ),
    RegisterError,
> {
    let (essential_metrics_recorder, essential_metrics_sampling_handle) =
        waymark_essential_metrics_sampler::recorder::new();
    let (prometheus_recorder, prometheus_handle) =
        waymark_prometheus_exporter_bringup::build().map_err(RegisterError::Build)?;

    let fanout = metrics_util::layers::FanoutBuilder::default()
        .add_recorder(essential_metrics_recorder)
        .add_recorder(prometheus_recorder)
        .build();

    metrics::set_global_recorder(fanout).map_err(|_| RegisterError::AlreadyInstalled)?;

    Ok((essential_metrics_sampling_handle, prometheus_handle))
}
