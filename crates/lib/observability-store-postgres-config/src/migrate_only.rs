//! A Postgres observability store that is only migrated.

use waymark_secret_string::SecretString;

use waymark_postgres_config::ConnectionConfig;

/// Configuration for a Postgres observability store that is only
/// migrated.
#[derive(Debug, Clone)]
pub struct MigrateOnly {
    /// The connection the migrations run through.
    pub migrations: ConnectionConfig,
}

/// Error returned when reading a [`MigrateOnly`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The migrations connection's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),
}

impl MigrateOnly {
    /// Create config from environment variables, for the store migrated
    /// at `migrations_url`.
    pub fn from_url_and_env(migrations_url: SecretString) -> Result<Self, FromEnvError> {
        let migrations = connection_from_url_and_env(migrations_url)?;

        Ok(Self { migrations })
    }
}

/// The migrations connection's config, for the store migrated at
/// `migrations_url`, with the rest from environment variables.
pub(crate) fn connection_from_url_and_env(
    migrations_url: SecretString,
) -> Result<ConnectionConfig, FromEnvError> {
    // 10 minutes: a migration on a large table is slow before it is
    // stuck.
    let statement_timeout = waymark_postgres_config::statement_timeout_from_env(
        "WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_STATEMENT_TIMEOUT_MS",
        "600000",
    )
    .map_err(FromEnvError::StatementTimeout)?;

    Ok(ConnectionConfig {
        url: migrations_url,
        statement_timeout,
    })
}
