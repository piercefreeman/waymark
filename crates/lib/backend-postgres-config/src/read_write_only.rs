//! The main database, read and written.

use waymark_postgres_config::PoolConfig;
use waymark_secret_string::SecretString;

use crate::{IntFromEnvError, MustUrlFromEnvError};

/// Configuration for the main database, read and written.
#[derive(Debug, Clone)]
pub struct ReadWriteOnly {
    /// The pool the runtime reads and writes through.
    pub read_write: PoolConfig,
}

/// Error returned by [`ReadWriteOnly::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] MustUrlFromEnvError),

    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] FromUrlAndEnvError),
}

/// Error returned by [`ReadWriteOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
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

        Self::from_url_and_env(url).map_err(FromEnvError::ReadWrite)
    }

    /// Create config for the database at `url`, with the rest from
    /// environment variables.
    pub fn from_url_and_env(url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let read_write = pool_from_url_and_env(url)?;

        Ok(Self { read_write })
    }
}

/// The read-write pool's config, for the database at `url`, with the rest
/// from environment variables.
pub(crate) fn pool_from_url_and_env(url: SecretString) -> Result<PoolConfig, FromUrlAndEnvError> {
    let max_connections = envfury::or_parse("WAYMARK_DATABASE_MAX_CONNECTIONS", "25")
        .map_err(FromUrlAndEnvError::MaxConnections)?;
    let statement_timeout = waymark_postgres_config::statement_timeout_from_env(
        "WAYMARK_DATABASE_STATEMENT_TIMEOUT_MS",
        "60000",
    )
    .map_err(FromUrlAndEnvError::StatementTimeout)?;

    Ok(PoolConfig {
        url,
        max_connections,
        statement_timeout,
    })
}
