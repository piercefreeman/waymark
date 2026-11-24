use std::time::Duration;

use color_eyre::eyre::WrapErr as _;
use waymark_secret_string::SecretString;

/// The schema the executor's observability bringup provisions its store
/// in. Hardcoded for now — mirrors `waymark-observability-bringup`.
const OBSERVABILITY_SCHEMA: &str = "observability";

/// Connect the observability store the executor writes to, migrated,
/// waiting up to `timeout` for its database to accept connections.
///
/// The database is resolved the way the executor resolves it — from the
/// store's own variables, with `dsn` as the unified URL the executor is
/// started with — so the harness resets and reads the store the executor
/// writes to. The store is migrated through
/// the executor's migrations connection, which creates the schema, under
/// the exclusive migrations lock, which is released afterwards: the
/// harness takes no migrations lock for its own statements. The returned
/// store's pool only requires the schema.
pub async fn connect(
    dsn: &SecretString,
    timeout: Duration,
) -> Result<waymark_observability_store_postgres::Store, color_eyre::eyre::Report> {
    let config =
        waymark_observability_config::ObservabilityConfig::from_env_with_unified_urls(dsn, dsn)
            .wrap_err("read the observability config")?;
    let waymark_observability_store_config::ReadWriteMigrate::Postgres(postgres_config) = config.db;
    let assertions = waymark_observability_store_postgres_migrations::assertions();
    let migrations = waymark_migrations_engine::validate_migrations_array(
        waymark_observability_store_postgres_migrations::migrations(),
    )
    .wrap_err("the observability store's migrations array")?;

    crate::common::wait_for_database("the observability database", timeout, || async {
        let migrated = waymark_postgres_bringup::migrate_and_release_migrations_lock::<
            waymark_observability_store_postgres_migrations::Tables,
            _,
            _,
            _,
        >(
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Params {
                migrations_connection: &postgres_config.migrations,
                migrations_lock: &postgres_config.migrations_lock,
                schema: Some(OBSERVABILITY_SCHEMA.into()),
                holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId::from_raw(
                    waymark_ids::NodeId::new_uuid_v4().into(),
                ),
                assertions: &assertions,
                migrations: &migrations,
                transactions: waymark_migrations_engine::MigrationsRunTransactions::WholeRun,
            },
        )
        .await;

        match migrated {
            Ok(_) => Ok(()),
            // TODO: when the database setup is reworked, classify these the
            // way the main database's wait does (`setup_db::is_permanent`,
            // with `42501` added to it), so a wrong password, a missing
            // database or a refused privilege stops the wait here too.
            Err(
                error @ waymark_postgres_bringup::migrate_and_release_migrations_lock::Error::ConnectForMigrations(
                    waymark_postgres_bringup::ConnectionError::Connect(_),
                ),
            ) => Err(crate::common::WaitForDatabaseAttemptError::Retry(error)),
            // A bad URL, a refused migrations lock, a migration failure or
            // a failed close is not the database coming up: nothing to wait
            // for, and no migration runs twice.
            Err(error) => Err(crate::common::WaitForDatabaseAttemptError::Stop(error)),
        }
    })
    .await
    .wrap_err("migrate the observability store before soak")?;

    let pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
        config: &postgres_config.write,
        transactions: waymark_postgres_bringup::Transactions::Unrestricted,
        schema: Some(waymark_postgres_bringup::Schema::Require(
            OBSERVABILITY_SCHEMA.into(),
        )),
    })
    .await
    .wrap_err("connect the observability schema pool")?;

    Ok(waymark_observability_store_postgres::Store { pool })
}
