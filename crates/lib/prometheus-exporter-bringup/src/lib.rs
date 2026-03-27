use std::net::SocketAddr;

pub fn spawn_and_install_recorder(
    metrics_addr: impl Into<SocketAddr>,
) -> Result<(), metrics_exporter_prometheus::BuildError> {
    let (recorder, future) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(metrics_addr)
        .set_bucket_duration(std::time::Duration::from_secs(600))?
        .build()?;

    metrics::set_global_recorder(recorder)?;

    tokio::spawn(future);

    Ok(())
}
