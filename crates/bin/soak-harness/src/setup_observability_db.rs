use std::time::Duration;

use color_eyre::eyre::WrapErr as _;
use waymark_secret_string::SecretString;

/// The schema the worker's observability bringup provisions its store
/// in. Hardcoded for now — mirrors `waymark-observability-bringup`.
const OBSERVABILITY_SCHEMA: &str = "observability";

/// Connect the observability store the worker writes to, waiting up to
/// `timeout` for its database to accept connections.
///
/// The database is resolved the way the worker resolves it — from the
/// environment, with `dsn` as the default — so the harness resets and
/// reads the store the worker writes to.
pub async fn connect(
    dsn: &SecretString,
    timeout: Duration,
) -> Result<waymark_observability_store_postgres::Store, color_eyre::eyre::Report> {
    let config = waymark_observability_config::ObservabilityConfig::from_env(dsn)
        .wrap_err("read the observability config")?;
    let waymark_observability_config::Db::Postgres(postgres_config) = config.db;

    let pool = crate::common::wait_for_database("the observability database", timeout, || async {
        match waymark_observability_store_postgres_bringup::schema_pool(
            &postgres_config.write,
            OBSERVABILITY_SCHEMA,
        )
        .await
        {
            Ok(pool) => Ok(pool),
            Err(error @ waymark_observability_store_postgres_bringup::SchemaPoolError::Url(_)) => {
                Err(crate::common::WaitForDatabaseAttemptError::Stop(error))
            }
            Err(waymark_observability_store_postgres_bringup::SchemaPoolError::Connect(error)) => {
                Err(crate::common::WaitForDatabaseAttemptError::Retry(error))
            }
        }
    })
    .await
    .wrap_err("connect the observability schema pool")?;

    Ok(waymark_observability_store_postgres::Store { pool })
}
