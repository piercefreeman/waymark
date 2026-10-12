//! A Postgres observability store that is only read.

use waymark_secret_string::SecretString;

use waymark_postgres_config::pool;

/// Configuration for a Postgres observability store that is only read.
#[derive(Debug, Clone)]
pub struct ReadOnly {
    /// The pool the query backends read through. Its URL may point at the
    /// store's database, or at a read replica of it.
    pub read: pool::Config,
}

/// Error returned when reading a [`ReadOnly`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The read pool's max-connections cap could not be read.
    #[error("max connections: {0}")]
    MaxConnections(#[source] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),

    /// The read pool's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),
}

impl ReadOnly {
    /// Create config from environment variables, for the store read at
    /// `read_url`.
    pub fn from_url_and_env(read_url: SecretString) -> Result<Self, FromEnvError> {
        let read = pool_from_url_and_env(read_url)?;

        Ok(Self { read })
    }
}

/// The read pool's config, for the store read at `read_url`, with the rest
/// from environment variables.
pub(crate) fn pool_from_url_and_env(read_url: SecretString) -> Result<pool::Config, FromEnvError> {
    let max_connections =
        envfury::or_parse("WAYMARK_OBSERVABILITY_POSTGRES_READ_MAX_CONNECTIONS", "4")
            .map_err(FromEnvError::MaxConnections)?;
    // 10 seconds: sized for paged, range-bound reads; a read the client
    // gave up on holds its connection no longer than this.
    let statement_timeout = waymark_postgres_config::statement_timeout_from_env(
        "WAYMARK_OBSERVABILITY_POSTGRES_READ_STATEMENT_TIMEOUT_MS",
        "10000",
    )
    .map_err(FromEnvError::StatementTimeout)?;

    Ok(pool::Config {
        url: read_url,
        max_connections,
        statement_timeout,
    })
}
