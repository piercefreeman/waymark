//! Bringup for the Prometheus exporter: the recorder and its exporter
//! server future, built together but not installed or spawned — the
//! caller decides where the recorder goes (e.g. into a fanout) and when
//! the server starts.

#![warn(missing_docs)]

use std::net::SocketAddr;

/// Build the Prometheus recorder and its exporter server future, serving
/// on `metrics_addr` — without installing or spawning anything. Serve
/// failures inside the returned future are logged.
///
/// Must be called within a tokio runtime: the recorder's upkeep task is
/// spawned here.
pub fn build(
    metrics_addr: SocketAddr,
) -> Result<
    (
        metrics_exporter_prometheus::PrometheusRecorder,
        impl Future<Output = ()>,
    ),
    metrics_exporter_prometheus::BuildError,
> {
    let (recorder, exporter) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(metrics_addr)
        .set_bucket_duration(std::time::Duration::from_secs(600))?
        // One ladder covers every `_seconds` histogram, so it has to span
        // the whole range they occupy: slot acquisition settles in tens of
        // microseconds, while action handling runs to the minute. A ladder
        // starting at 0.1 puts every acquisition observation in the first
        // bucket, where `histogram_quantile` can only interpolate across
        // `[0, 0.1]` and answers ~0.05 for a median three orders of
        // magnitude below it. The decade steps below 0.1 give the low end
        // boundaries to interpolate between.
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_seconds".to_string()),
            &[
                1e-5, 3e-5, 1e-4, 3e-4, 1e-3, 3e-3, 1e-2, 3e-2, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.,
                30., 60., 300., 600.,
            ],
        )?
        .build()?;

    let exporter = async move {
        if let Err(error) = exporter.await {
            tracing::error!(?error, "prometheus exporter exited");
        }
    };

    Ok((recorder, exporter))
}
