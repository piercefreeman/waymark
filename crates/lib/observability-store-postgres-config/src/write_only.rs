//! A Postgres observability store that is only written.

use waymark_secret_string::SecretString;

use waymark_postgres_config::PoolConfig;

/// Configuration for a Postgres observability store that is only written.
#[derive(Debug, Clone)]
pub struct WriteOnly {
    /// The pool the sinks and the retention sweeps write through.
    pub write: PoolConfig,
}

/// Error returned when reading a [`WriteOnly`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The write pool's max-connections cap could not be read.
    #[error("max connections: {0}")]
    MaxConnections(#[source] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),

    /// The write pool's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),
}

impl WriteOnly {
    /// Create config from environment variables, for the store written
    /// at `write_url`.
    pub fn from_url_and_env(write_url: SecretString) -> Result<Self, FromEnvError> {
        let write = pool_from_url_and_env(write_url)?;

        Ok(Self { write })
    }
}

/// The write pool's config, for the store written at `write_url`, with the
/// rest from environment variables.
pub(crate) fn pool_from_url_and_env(write_url: SecretString) -> Result<PoolConfig, FromEnvError> {
    let max_connections = envfury::or_parse("WAYMARK_OBSERVABILITY_POSTGRES_MAX_CONNECTIONS", "4")
        .map_err(FromEnvError::MaxConnections)?;
    // TODO: measure the longest legitimate write (a sink flush, a chunk of
    // a retention sweep) and size this from it.
    let statement_timeout = waymark_postgres_config::statement_timeout_from_env(
        "WAYMARK_OBSERVABILITY_POSTGRES_STATEMENT_TIMEOUT_MS",
        "30000",
    )
    .map_err(FromEnvError::StatementTimeout)?;

    Ok(PoolConfig {
        url: write_url,
        max_connections,
        statement_timeout,
    })
}
