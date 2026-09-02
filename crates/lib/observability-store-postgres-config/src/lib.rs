//! Config for a Postgres observability store.

use std::num::NonZeroU32;

use waymark_secret_string::SecretString;

/// Configuration for a Postgres observability store.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// The database URL.
    pub url: SecretString,

    /// Connection cap for each pool built from this config. Observability
    /// pools are always the consumer's own — never the main database
    /// pool — so a slow observability write can only ever wait on this
    /// budget.
    ///
    /// Note for sizing: when the observability store shares the main
    /// database (the default), these connections are ADDITIVE — the
    /// server sees the main pool's connections plus this cap for every
    /// observability pool, so the main pool's own cap understates the
    /// total.
    pub max_connections: NonZeroU32,
}

/// Error returned when reading a [`PostgresConfig`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The max-connections cap could not be read.
    #[error(transparent)]
    MaxConnections(#[from] envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),
}

impl PostgresConfig {
    /// Create config from environment variables, for the store at `url`.
    ///
    /// The URL is a parameter rather than a variable of its own: the
    /// backend-neutral `WAYMARK_OBSERVABILITY_DATABASE_URL` is read and
    /// dispatched by scheme in `waymark-observability-config`.
    pub fn from_env(url: SecretString) -> Result<Self, FromEnvError> {
        let max_connections =
            envfury::or_parse("WAYMARK_OBSERVABILITY_POSTGRES_MAX_CONNECTIONS", "4")?;
        Ok(Self {
            url,
            max_connections,
        })
    }
}
