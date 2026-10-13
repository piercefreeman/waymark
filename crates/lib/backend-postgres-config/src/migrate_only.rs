//! The main database, migrated.

use waymark_postgres_config::ConnectionConfig;
use waymark_secret_string::SecretString;

use crate::{IntFromEnvError, MustUrlFromEnvError, UrlFromEnvError};

/// Configuration for the main database, migrated.
#[derive(Debug, Clone)]
pub struct MigrateOnly {
    /// The connection the migrations run through. Its URL may carry other
    /// credentials than the database URL's, with the rights the
    /// migrations need.
    pub migrations: ConnectionConfig,
}

/// Error returned by [`MigrateOnly::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL, which the migrations database URL defaults to,
    /// could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] MustUrlFromEnvError),

    /// The migrations database URL could not be read.
    #[error("migrations database url: {0}")]
    MigrationsDatabaseUrl(#[source] UrlFromEnvError),

    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] FromUrlAndEnvError),
}

/// Error returned by [`MigrateOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The migrations connection's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] IntFromEnvError),
}

impl MigrateOnly {
    /// Create config from environment variables. `WAYMARK_DATABASE_URL` is
    /// used when `WAYMARK_MIGRATIONS_DATABASE_URL` is not set.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let url = crate::url_from_env().map_err(FromEnvError::DatabaseUrl)?;
        let migrations_url = crate::migrations_url_from_env_with_default(&url)
            .map_err(FromEnvError::MigrationsDatabaseUrl)?;

        Self::from_url_and_env(migrations_url).map_err(FromEnvError::Migrations)
    }

    /// Create config for the database migrated at `migrations_url`, with
    /// the rest from environment variables.
    pub fn from_url_and_env(migrations_url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let migrations = connection_from_url_and_env(migrations_url)?;

        Ok(Self { migrations })
    }
}

/// The migrations connection's config, for the database migrated at
/// `migrations_url`, with the rest from environment variables.
pub(crate) fn connection_from_url_and_env(
    migrations_url: SecretString,
) -> Result<ConnectionConfig, FromUrlAndEnvError> {
    // 10 minutes: a migration on a large table is slow before it is
    // stuck.
    let statement_timeout = waymark_postgres_config::statement_timeout_from_env(
        "WAYMARK_MIGRATIONS_STATEMENT_TIMEOUT_MS",
        "600000",
    )
    .map_err(FromUrlAndEnvError::StatementTimeout)?;

    Ok(ConnectionConfig {
        url: migrations_url,
        statement_timeout,
    })
}
