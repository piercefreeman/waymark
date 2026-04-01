use std::net::SocketAddr;

pub fn spawn_and_install_recorder(
    metrics_addr: impl Into<SocketAddr>,
) -> Result<(), metrics_exporter_prometheus::BuildError> {
    let (recorder, future) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(metrics_addr)
        .set_bucket_duration(std::time::Duration::from_secs(600))?
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_seconds".to_string()),
            &[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10., 30., 60., 300., 600.],
        )?
        .build()?;

    metrics::set_global_recorder(recorder)?;

    tokio::spawn(future);

    Ok(())
}
