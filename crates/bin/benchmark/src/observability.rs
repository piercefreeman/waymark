//! The observability subsystem under the benchmark: brought up on
//! request, so a run pays for the events pipeline and the essential
//! metrics sampler exactly as a worker node does.

use std::sync::Arc;

use color_eyre::eyre::WrapErr as _;
use waymark_secret_string::SecretStr;

/// The observability schema: the one this benchmark resets.
const SCHEMA: &str = "observability";

/// The observability subsystem of one benchmark run.
pub struct Observability {
    /// The node's event emitter, for the VM driver hooks.
    pub emitter: Arc<waymark_observability_bringup::Emitter>,

    /// What the VM driver hooks record.
    pub vm_driver_hooks_policy: waymark_observability_events_vm_driver_hooks::Policy,

    /// A store over the observability schema, emptied at the start, for
    /// counting the events the run recorded.
    store: waymark_observability_store_postgres::Store,
}

/// Bring the observability subsystem up over the observability database at
/// `dsn` — written and read there; the observability database URL
/// variables are not read — with its tables emptied first, ending on
/// `shutdown_token`; its pipelines are supervised by `spawner`.
///
/// The metrics recorder is installed process-wide here, with the
/// Prometheus exporter on an ephemeral port nobody scrapes: the
/// essential-metrics sampler needs the recorder, not the exporter.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    dsn: &SecretStr,
    node_id: waymark_ids::NodeId,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<Observability, color_eyre::eyre::Report>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    let sampling_handle = waymark_metrics_bringup::start(([127, 0, 0, 1], 0))
        .wrap_err("install the metrics recorder")?;

    let config = waymark_observability_config::ObservabilityConfig::from_urls_and_env(
        dsn.into(),
        dsn.into(),
    )
    .wrap_err("read the observability config")?;

    // The store's pool comes from the config, so the reset and the count
    // hit the database the pipelines write to.
    let waymark_observability_config::Db::Postgres(postgres_config) = &config.db;
    let pool =
        waymark_observability_store_postgres_bringup::schema_pool(&postgres_config.write, SCHEMA)
            .await
            .wrap_err("connect the observability schema pool")?;
    let store = waymark_observability_store_postgres::Store { pool };
    // Empty the store so that every run measures over an empty store.
    // Provision before the reset: `truncate_all` requires a migrated store,
    // and the bringup's own migration run comes after the reset.
    waymark_observability_store_postgres_migrations::run(&store.pool)
        .await
        .wrap_err("migrate the observability store")?;
    waymark_observability_store_postgres::reset::truncate_all(&store.pool)
        .await
        .wrap_err("clear the observability tables")?;

    let (_api_router, emitter, vm_driver_hooks_policy) = waymark_observability_bringup::start(
        &mut spawner,
        config,
        node_id,
        sampling_handle,
        shutdown_token,
    )
    .await
    .wrap_err("start the observability subsystem")?;

    Ok(Observability {
        emitter: Arc::new(emitter),
        vm_driver_hooks_policy,
        store,
    })
}

/// Count the events the run recorded, once its pipelines have been
/// drained.
pub async fn recorded_events(
    observability: Observability,
) -> Result<i64, color_eyre::eyre::Report> {
    let Observability {
        emitter: _,
        vm_driver_hooks_policy: _,
        store,
    } = observability;

    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM observability_events")
        .fetch_one(&store.pool)
        .await
        .wrap_err("count the recorded events")?;

    Ok(events)
}
