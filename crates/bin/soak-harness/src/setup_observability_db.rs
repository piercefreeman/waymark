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
/// environment, with `dsn` as the default — so the harness resets and
/// reads the store the executor writes to. The store is migrated the way
/// the executor migrates it: through the migrations connection, which
/// creates the schema; the returned store's pool only requires it.
pub async fn connect(
    dsn: &SecretString,
    timeout: Duration,
) -> Result<waymark_observability_store_postgres::Store, color_eyre::eyre::Report> {
    let config =
        waymark_observability_config::ObservabilityConfig::from_env_urls_with_default(dsn, dsn)
            .wrap_err("read the observability config")?;
    let waymark_observability_store_config::ReadWriteMigrate::Postgres(postgres_config) = config.db;

    crate::common::wait_for_database("the observability database", timeout, || async {
        let migrated = waymark_postgres_bringup::migrate(
            &postgres_config.migrations,
            Some(OBSERVABILITY_SCHEMA.into()),
            async |connection| {
                waymark_observability_store_postgres_migrations::run(connection).await
            },
        )
        .await;

        match migrated {
            Ok(()) => Ok(()),
            // TODO: when the database setup is reworked, classify these the
            // way the main database's wait does (`setup_db::is_permanent`,
            // with `42501` added to it), so a wrong password, a missing
            // database or a refused privilege stops the wait here too.
            Err(
                error @ waymark_postgres_bringup::MigrateError::Connect(
                    waymark_postgres_bringup::ConnectionError::Connect(_),
                ),
            ) => Err(crate::common::WaitForDatabaseAttemptError::Retry(error)),
            // A bad URL, a migration failure or a failed close is not the
            // database coming up: nothing to wait for, and no migration runs
            // twice.
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
