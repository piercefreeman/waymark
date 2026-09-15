//! Bringup for the Prometheus exporter: the recorder, built here and
//! installed by the caller, and the `/metrics` server and the upkeep
//! over it, as tasks of the spawner the caller hands in.

#![warn(missing_docs)]

/// How often the recorder's upkeep runs.
const UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Build the Prometheus recorder — without installing it — and return it
/// with its handle, for [`start`].
pub fn build() -> Result<
    (
        metrics_exporter_prometheus::PrometheusRecorder,
        metrics_exporter_prometheus::PrometheusHandle,
    ),
    metrics_exporter_prometheus::BuildError,
> {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_recommended_naming(true)
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
        .build_recorder();
    let handle = recorder.handle();

    Ok((recorder, handle))
}

/// Start the `/metrics` server on `metrics_addr` and the upkeep of the
/// recorder behind `handle`, as tasks of `spawner` ending on
/// `shutdown_token`.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    metrics_addr: std::net::SocketAddr,
    handle: metrics_exporter_prometheus::PrometheusHandle,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(), waymark_http_bringup::NamedStartError>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    waymark_http_bringup::start(
        &mut spawner,
        "metrics http server",
        metrics_addr,
        router(handle.clone()),
        shutdown_token.clone().cancelled_owned(),
    )
    .await?;

    spawner.spawn(
        "prometheus upkeep",
        upkeep(handle, shutdown_token.child_token()),
    );

    Ok(())
}

/// The `/metrics` route: the recorder behind `handle`, rendered.
pub fn router(handle: metrics_exporter_prometheus::PrometheusHandle) -> axum::Router {
    axum::Router::new().route(
        "/metrics",
        axum::routing::get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    )
}

/// Run the upkeep of the recorder behind `handle` every
/// `UPKEEP_INTERVAL`, until `shutdown_token` is cancelled.
pub async fn upkeep(
    handle: metrics_exporter_prometheus::PrometheusHandle,
    shutdown_token: tokio_util::sync::CancellationToken,
) {
    shutdown_token
        .run_until_cancelled(async {
            loop {
                tokio::time::sleep(UPKEEP_INTERVAL).await;
                handle.run_upkeep();
            }
        })
        .await;
}
