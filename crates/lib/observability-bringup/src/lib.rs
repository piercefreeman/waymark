//! Bringup for observability: the store, and each observability
//! subsystem's bringup plugged in over it.

#![warn(missing_docs)]

use std::sync::Arc;

use waymark_observability_config::ObservabilityConfig;

/// The observability schema: one schema for every observability
/// subsystem's tables.
const SCHEMA: &str = "observability";

/// The node's event emitter, over the store's payload.
pub type Emitter =
    waymark_observability_events_bringup::EmitterFor<waymark_observability_store_postgres::Store>;

/// Error returned by [`start`].
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The store could not be migrated.
    #[error("migrating the observability store: {0}")]
    Migrate(#[source] waymark_postgres_bringup::MigrateError<sqlx::migrate::MigrateError>),

    /// The observability write pool could not be brought up.
    #[error("bringing up the observability write pool: {0}")]
    WritePool(#[source] waymark_postgres_bringup::PoolError),

    /// The observability read pool could not be brought up.
    #[error("bringing up the observability read pool: {0}")]
    ReadPool(#[source] waymark_postgres_bringup::PoolError),
}

/// Bring up the observability store — migrated through a connection of
/// its own, then a write pool and a read pool — and every observability
/// subsystem's pipeline over the
/// write side, as tasks of `spawner` ending on `shutdown_token` — the
/// lossy batchers on their last handle or on `force_shutdown_token`;
/// returns the observability API router over the read side, and the
/// node's event emitter for producers to share, with what the VM driver
/// hooks record through it.
///
/// `handle` is the sampling half of the essential-metrics recorder pair;
/// the recording half must already be installed in the process-global
/// fanout.
pub async fn start<Spawner>(
    mut spawner: Spawner,
    config: ObservabilityConfig,
    node_id: waymark_ids::NodeId,
    handle: waymark_essential_metrics_sampler::recorder::Handle,
    shutdown_token: tokio_util::sync::CancellationToken,
    force_shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<
    (
        aide::axum::ApiRouter,
        Emitter,
        waymark_observability_events_vm_driver_hooks::Policy,
    ),
    StartError,
>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    let waymark_observability_store_config::ReadWriteMigrate::Postgres(postgres_config) =
        &config.db;

    // The migrations first, on a connection of their own: it alone creates
    // the schema, and its statement timeout and credentials are the
    // migrations', not the writes'. The pools then require the schema and
    // never create it: the write pool's credentials need no rights beyond
    // the writes, and the read pool may point at a replica, where nothing
    // can be created, and which only sees the schema once the primary has
    // it and it has caught up.
    waymark_postgres_bringup::migrate(
        &postgres_config.migrations,
        Some(SCHEMA.into()),
        async |connection| waymark_observability_store_postgres_migrations::run(connection).await,
    )
    .await
    .map_err(StartError::Migrate)?;

    // The store's pools are its own, never the main database's: a slow
    // observability statement waits only on its own pool's budget, and a
    // runaway read exhausts only the read pool. When the store shares the
    // main database, the server sees the main pool's connections plus the
    // cap of each pool here.
    let write_pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
        config: &postgres_config.write,
        transactions: waymark_postgres_bringup::Transactions::Unrestricted,
        schema: Some(waymark_postgres_bringup::Schema::Require(SCHEMA.into())),
    })
    .await
    .map_err(StartError::WritePool)?;

    let read_pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
        config: &postgres_config.read,
        transactions: waymark_postgres_bringup::Transactions::ReadOnly,
        schema: Some(waymark_postgres_bringup::Schema::Require(SCHEMA.into())),
    })
    .await
    .map_err(StartError::ReadPool)?;

    let write_store = Arc::new(waymark_observability_store_postgres::Store { pool: write_pool });
    let read_store = Arc::new(waymark_observability_store_postgres::Store { pool: read_pool });

    let essential_metrics_api_router = waymark_essential_metrics_bringup::start(
        &mut spawner,
        config.essential_metrics,
        node_id,
        handle,
        Arc::clone(&write_store),
        Arc::clone(&read_store),
        shutdown_token.clone(),
        force_shutdown_token.clone(),
    );

    let (observability_events_api_router, emitter, vm_driver_hooks_policy) =
        waymark_observability_events_bringup::start(
            &mut spawner,
            config.observability_events,
            node_id,
            write_store,
            Arc::clone(&read_store),
            shutdown_token,
            force_shutdown_token,
        );

    let observability_state_api_router = waymark_api_observability_state_http::router(read_store);

    let api_router = aide::axum::ApiRouter::new()
        .merge(essential_metrics_api_router)
        .merge(observability_events_api_router)
        .merge(observability_state_api_router);

    Ok((api_router, emitter, vm_driver_hooks_policy))
}

/// Error returned by [`start_emitter_only`].
#[derive(Debug, thiserror::Error)]
pub enum StartEmitterOnlyError {
    /// The store's schema is not there, or not migrated exactly to this
    /// build's migrations.
    #[error("checking the observability migrations: {0}")]
    MigrationsCheck(
        #[source]
        waymark_postgres_bringup::CheckMigrationsError<
            waymark_sqlx_postgres_migrations::CheckError,
        >,
    ),

    /// The observability write pool could not be brought up.
    #[error("bringing up the observability write pool: {0}")]
    WritePool(#[source] waymark_postgres_bringup::PoolError),
}

/// Bring up the observability-events producer side alone, for a process
/// that writes events but neither migrates the store nor serves the
/// observability API: a write pool over the store `config` describes,
/// which has to be migrated exactly to this build's migrations already,
/// and the lossy batcher into it under `lossy_batcher_policy`, as a task of
/// `spawner` that stops when the emitter's last handle is dropped or on
/// `force_shutdown_token` — and return `node_id`'s event emitter.
pub async fn start_emitter_only<Spawner>(
    spawner: Spawner,
    config: &waymark_observability_store_config::WriteOnly,
    lossy_batcher_policy: waymark_lossy_batcher::ValidPolicy,
    node_id: waymark_ids::NodeId,
    force_shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<Emitter, StartEmitterOnlyError>
where
    Spawner: waymark_managed_spawner::Spawner,
{
    let waymark_observability_store_config::WriteOnly::Postgres(postgres_config) = config;

    // The migrations check first, on a connection of its own: it waits for
    // a migration in progress, which the write pool's statement timeout
    // would cut short. A store migrated to other migrations than this
    // build's fails the bringup rather than losing every event written to
    // it. The connection and the pool require the schema and never create
    // it: only a process that migrates the store creates it.
    waymark_postgres_bringup::check_migrations(
        &postgres_config.migrations_check,
        Some(SCHEMA.into()),
        async |connection| waymark_observability_store_postgres_migrations::check(connection).await,
    )
    .await
    .map_err(StartEmitterOnlyError::MigrationsCheck)?;

    let write_pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
        config: &postgres_config.write,
        transactions: waymark_postgres_bringup::Transactions::Unrestricted,
        schema: Some(waymark_postgres_bringup::Schema::Require(SCHEMA.into())),
    })
    .await
    .map_err(StartEmitterOnlyError::WritePool)?;

    let write_store = Arc::new(waymark_observability_store_postgres::Store { pool: write_pool });

    let emitter = waymark_observability_events_bringup::start_emitter(
        spawner,
        lossy_batcher_policy,
        node_id,
        write_store,
        force_shutdown_token,
    );

    Ok(emitter)
}
