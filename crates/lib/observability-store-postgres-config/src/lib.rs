//! Config for a Postgres observability store.

use std::num::NonZeroU32;

use waymark_secret_string::SecretString;

/// Configuration for a Postgres observability store.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// The pool the sinks and the retention sweeps write through.
    pub write: PoolConfig,

    /// The pool the query backends read through. Its URL may name the
    /// same database as the write pool's, or a read replica of it.
    pub read: PoolConfig,
}

/// Configuration for one pool of a Postgres observability store.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// The database URL.
    pub url: SecretString,

    /// Connection cap for the pool. Observability pools are always the
    /// consumer's own — never the main database pool — so a slow
    /// observability statement can only ever wait on this budget, and a
    /// runaway read can only ever exhaust the read pool.
    ///
    /// Note for sizing: when the observability store shares the main
    /// database (the default), these connections are ADDITIVE — the
    /// server sees the main pool's connections plus the cap of every
    /// observability pool, so the main pool's own cap understates the
    /// total.
    pub max_connections: NonZeroU32,
}

/// Error returned when reading a [`PostgresConfig`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The write pool's max-connections cap could not be read.
    #[error(transparent)]
    WriteMaxConnections(envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),

    /// The read pool's max-connections cap could not be read.
    #[error(transparent)]
    ReadMaxConnections(envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),
}

impl PostgresConfig {
    /// Create config from environment variables, for the store written
    /// at `write_url` and read at `read_url`.
    ///
    /// The URLs are parameters rather than variables of their own: the
    /// backend-neutral `WAYMARK_OBSERVABILITY_DATABASE_URL` and
    /// `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` are read and dispatched
    /// by scheme in `waymark-observability-config`.
    pub fn from_env(write_url: SecretString, read_url: SecretString) -> Result<Self, FromEnvError> {
        let write_max_connections =
            envfury::or_parse("WAYMARK_OBSERVABILITY_POSTGRES_MAX_CONNECTIONS", "4")
                .map_err(FromEnvError::WriteMaxConnections)?;
        let read_max_connections =
            envfury::or_parse("WAYMARK_OBSERVABILITY_POSTGRES_READ_MAX_CONNECTIONS", "4")
                .map_err(FromEnvError::ReadMaxConnections)?;
        Ok(Self {
            write: PoolConfig {
                url: write_url,
                max_connections: write_max_connections,
            },
            read: PoolConfig {
                url: read_url,
                max_connections: read_max_connections,
            },
        })
    }
}
