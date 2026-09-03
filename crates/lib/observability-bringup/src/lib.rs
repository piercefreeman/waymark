//! Bringup for observability: the store, and each observability
//! subsystem's bringup plugged in over it.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_observability_config::{Db, ObservabilityConfig};

/// The observability schema: one schema for every observability
/// subsystem's tables.
const SCHEMA: &str = "observability";

/// The spawned observability tasks.
#[derive(Debug)]
pub struct Handles {
    /// The essential-metrics subsystem's tasks.
    pub essential_metrics: waymark_essential_metrics_bringup::Handles,
}

/// Error returned by [`start`].
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The observability pool could not be brought up.
    #[error("bringing up the observability pool: {0}")]
    SchemaPool(#[source] waymark_observability_store_postgres_bringup::SchemaPoolError),

    /// The store migrations failed.
    #[error("migrating the observability store: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
}

/// Bring up the observability store — the schema-scoped pool and its
/// migrations — and every observability subsystem's pipeline over it,
/// all ending on `shutdown_token`; returns the observability API router
/// merged from the observability subsystems' routers alongside the task
/// handles.
///
/// `handle` is the sampling half of the essential-metrics recorder pair;
/// the recording half must already be installed in the process-global
/// fanout.
pub async fn start(
    config: ObservabilityConfig,
    node_id: waymark_ids::NodeId,
    handle: waymark_essential_metrics_sampler::recorder::Handle,
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<(Handles, aide::axum::ApiRouter), StartError> {
    let Db::Postgres(postgres_config) = &config.db;

    let pool = waymark_observability_store_postgres_bringup::schema_pool(postgres_config, SCHEMA)
        .await
        .map_err(StartError::SchemaPool)?;

    waymark_observability_store_postgres_migrations::MIGRATOR
        .run(&pool)
        .await
        .map_err(StartError::Migrate)?;

    let store = Arc::new(waymark_observability_store_postgres::Store { pool });

    let (essential_metrics, essential_metrics_api_router) =
        waymark_essential_metrics_bringup::start(
            config.essential_metrics,
            node_id,
            handle,
            store,
            shutdown_token,
        );

    let api_router = aide::axum::ApiRouter::new().merge(essential_metrics_api_router);

    Ok((Handles { essential_metrics }, api_router))
}
