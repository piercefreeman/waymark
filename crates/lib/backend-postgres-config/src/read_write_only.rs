//! The main database, read and written.

use waymark_postgres_config::{connection, pool};
use waymark_secret_string::SecretString;

use crate::{IntFromEnvError, MustUrlFromEnvError};

/// Configuration for the main database, read and written.
#[derive(Debug, Clone)]
pub struct ReadWriteOnly {
    /// The pool the runtime reads and writes through.
    pub read_write: pool::Config,

    /// The connection the migrations check runs through, to the read-write
    /// pool's URL. The check waits for a migration in progress, so the
    /// connection's statement timeout is the migrations connection's.
    pub migrations_check: connection::Config,
}

/// Error returned by [`ReadWriteOnly::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] MustUrlFromEnvError),

    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] PoolFromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] crate::migrate_only::FromUrlAndEnvError),
}

/// Error returned by [`ReadWriteOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] PoolFromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] crate::migrate_only::FromUrlAndEnvError),
}

/// Error returned when reading the read-write pool's config.
#[derive(Debug, thiserror::Error)]
pub enum PoolFromUrlAndEnvError {
    /// The pool's max-connections cap could not be read.
    #[error("max connections: {0}")]
    MaxConnections(#[source] IntFromEnvError),

    /// The pool's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] IntFromEnvError),
}

impl ReadWriteOnly {
    /// Create config from environment variables.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let url = crate::url_from_env().map_err(FromEnvError::DatabaseUrl)?;
        let read_write = pool_from_url_and_env(url.clone()).map_err(FromEnvError::ReadWrite)?;
        let migrations_check = crate::migrate_only::connection_from_url_and_env(url)
            .map_err(FromEnvError::MigrationsCheck)?;

        Ok(Self {
            read_write,
            migrations_check,
        })
    }

    /// Create config for the database at `url`, with the rest from
    /// environment variables.
    pub fn from_url_and_env(url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let read_write =
            pool_from_url_and_env(url.clone()).map_err(FromUrlAndEnvError::ReadWrite)?;
        let migrations_check = crate::migrate_only::connection_from_url_and_env(url)
            .map_err(FromUrlAndEnvError::MigrationsCheck)?;

        Ok(Self {
            read_write,
            migrations_check,
        })
    }
}

/// The read-write pool's config, for the database at `url`, with the rest
/// from environment variables.
pub(crate) fn pool_from_url_and_env(
    url: SecretString,
) -> Result<pool::Config, PoolFromUrlAndEnvError> {
    let max_connections = envfury::or_parse("WAYMARK_DATABASE_MAX_CONNECTIONS", "25")
        .map_err(PoolFromUrlAndEnvError::MaxConnections)?;
    let statement_timeout = waymark_postgres_config::statement_timeout_from_env(
        "WAYMARK_DATABASE_STATEMENT_TIMEOUT_MS",
        "60000",
    )
    .map_err(PoolFromUrlAndEnvError::StatementTimeout)?;

    Ok(pool::Config {
        url,
        max_connections,
        statement_timeout,
    })
}
