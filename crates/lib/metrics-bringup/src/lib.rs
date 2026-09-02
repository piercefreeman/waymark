//! Bringup for the process-global metrics recorder: the essential-metrics
//! recorder and the Prometheus recorder composed into one fanout,
//! installed once — the only `set_global_recorder` call in the system —
//! with the Prometheus exporter server spawned after the install.
//!
//! Only registration happens here: the returned sampling handle is the
//! essential-metrics recorder's counterpart, for the observability
//! bringup to run the actual pipeline over.

#![warn(missing_docs)]

use std::net::SocketAddr;

/// Error returned by [`start`].
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The Prometheus exporter could not be built.
    #[error("building the prometheus exporter: {0}")]
    Build(#[source] metrics_exporter_prometheus::BuildError),

    /// A process-global metrics recorder is already installed.
    #[error("a process-global metrics recorder is already installed")]
    AlreadyInstalled,
}

/// Install the process-global metrics recorder — a fanout of the
/// essential-metrics recorder and the Prometheus recorder — and spawn
/// the Prometheus exporter server. Metrics bound after this call land
/// in both.
///
/// Returns the essential-metrics sampling handle: the recorder's
/// read-side counterpart, consumed by the observability bringup.
pub fn start(
    metrics_addr: impl Into<SocketAddr>,
) -> Result<waymark_essential_metrics_sampler::recorder::Handle, StartError> {
    let (essential_metrics_recorder, essential_metrics_sampling_handle) =
        waymark_essential_metrics_sampler::recorder::new();
    let (prometheus_recorder, prometheus_exporter) =
        waymark_prometheus_exporter_bringup::build(metrics_addr.into())
            .map_err(StartError::Build)?;

    let fanout = metrics_util::layers::FanoutBuilder::default()
        .add_recorder(essential_metrics_recorder)
        .add_recorder(prometheus_recorder)
        .build();

    metrics::set_global_recorder(fanout).map_err(|_| StartError::AlreadyInstalled)?;

    tokio::spawn(prometheus_exporter);

    Ok(essential_metrics_sampling_handle)
}
