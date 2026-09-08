//! The observability subsystem under the benchmark: brought up on
//! request, so a run pays for the events pipeline and the essential
//! metrics sampler exactly as a worker node does.

use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::WrapErr as _;
use waymark_secret_string::SecretStr;

/// The observability schema: the one the observability bringup scopes
/// its store to.
const SCHEMA: &str = "observability";

/// The observability subsystem of one benchmark run.
pub struct Observability {
    /// The spawned pipelines.
    pub handles: waymark_observability_bringup::Handles,

    /// The node's event emitter, for the VM driver hooks.
    pub emitter: Arc<waymark_observability_bringup::Emitter>,

    /// What the VM driver hooks record.
    pub vm_driver_hooks_policy: waymark_observability_events_vm_driver_hooks::Policy,

    /// A store over the observability schema, for counting what the run
    /// recorded.
    store: waymark_observability_store_postgres::Store,
}

/// Bring the observability subsystem up over the benchmark database,
/// with its tables emptied first, ending on `shutdown_token`.
///
/// The metrics recorder is installed process-wide here, with the
/// Prometheus exporter on an ephemeral port nobody scrapes: the
/// essential-metrics sampler needs the recorder, not the exporter.
pub async fn start(
    dsn: &SecretStr,
    node_id: waymark_ids::NodeId,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<Observability, color_eyre::eyre::Report> {
    let sampling_handle = waymark_metrics_bringup::start(([127, 0, 0, 1], 0))
        .wrap_err("install the metrics recorder")?;

    let config = waymark_observability_config::ObservabilityConfig::from_env(&dsn.into())
        .wrap_err("read the observability config")?;

    let pool = waymark_sqlx_postgres_schema_pool::connect(dsn.expose_secret(), SCHEMA)
        .await
        .wrap_err("connect the observability schema pool")?;
    let store = waymark_observability_store_postgres::Store { pool };
    waymark_observability_store_postgres::reset::truncate_all(&store.pool)
        .await
        .wrap_err("clear the observability tables")?;

    let (handles, _api_router, emitter, vm_driver_hooks_policy) =
        waymark_observability_bringup::start(config, node_id, sampling_handle, shutdown_token)
            .await
            .wrap_err("start the observability subsystem")?;

    Ok(Observability {
        handles,
        emitter: Arc::new(emitter),
        vm_driver_hooks_policy,
        store,
    })
}

/// Wait for the pipelines to end — their token is cancelled by the
/// caller — and count the events the run recorded.
pub async fn shutdown(observability: Observability) -> Result<u64, color_eyre::eyre::Report> {
    let Observability {
        handles,
        emitter,
        vm_driver_hooks_policy: _,
        store,
    } = observability;
    drop(emitter);

    let _ = tokio::time::timeout(Duration::from_secs(5), handles.essential_metrics.sampler).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), handles.essential_metrics.batcher).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), handles.essential_metrics.retention).await;
    let _ =
        tokio::time::timeout(Duration::from_secs(5), handles.observability_events.batcher).await;
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        handles.observability_events.retention,
    )
    .await;

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM observability_events")
        .fetch_one(&store.pool)
        .await
        .wrap_err("count the recorded events")?;

    Ok(u64::try_from(events).unwrap_or(0))
}
