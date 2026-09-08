//! Config for a Postgres observability store.

use std::num::{NonZeroU32, NonZeroU64};

use waymark_nonzero_duration::NonZeroDuration;
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

    /// How long one statement may run before the server ends it. A
    /// statement the client gave up on keeps running otherwise, holding
    /// its connection; this is the hard end to it.
    pub statement_timeout: NonZeroDuration,
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

    /// The write pool's statement timeout could not be read.
    #[error(transparent)]
    WriteStatementTimeout(envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),

    /// The read pool's statement timeout could not be read.
    #[error(transparent)]
    ReadStatementTimeout(envfury::Error<envfury::OrParseError<std::num::ParseIntError>>),
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
        // 10 minutes: a retention sweep or a migration on a large table
        // is slow before it is stuck.
        let write_statement_timeout_millis: NonZeroU64 = envfury::or_parse(
            "WAYMARK_OBSERVABILITY_POSTGRES_STATEMENT_TIMEOUT_MS",
            "600000",
        )
        .map_err(FromEnvError::WriteStatementTimeout)?;
        // 10 seconds: every read is bounded by its page and its range.
        let read_statement_timeout_millis: NonZeroU64 = envfury::or_parse(
            "WAYMARK_OBSERVABILITY_POSTGRES_READ_STATEMENT_TIMEOUT_MS",
            "10000",
        )
        .map_err(FromEnvError::ReadStatementTimeout)?;
        Ok(Self {
            write: PoolConfig {
                url: write_url,
                max_connections: write_max_connections,
                statement_timeout: NonZeroDuration::from_nonzero_millis(
                    write_statement_timeout_millis,
                ),
            },
            read: PoolConfig {
                url: read_url,
                max_connections: read_max_connections,
                statement_timeout: NonZeroDuration::from_nonzero_millis(
                    read_statement_timeout_millis,
                ),
            },
        })
    }
}
