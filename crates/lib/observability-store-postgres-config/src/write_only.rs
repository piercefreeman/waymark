//! A Postgres observability store that is only written.

use waymark_secret_string::SecretString;

use waymark_postgres_config::{migrations_lock, pool};

use crate::profiles;

/// Configuration for a Postgres observability store that is only written.
#[derive(Debug, Clone)]
pub struct WriteOnly {
    /// The pool the sinks and the retention sweeps write through.
    pub write: pool::Config,

    /// The shared migrations lock the store is written under, through
    /// the write pool's URL.
    pub migrations_lock: migrations_lock::Config,
}

/// Error returned by [`WriteOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The write pool's config could not be read.
    #[error("write pool: {0}")]
    Write(#[source] pool::FromUrlAndEnvError),

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
}

impl WriteOnly {
    /// Create config from environment variables, for the store written
    /// at `write_url`.
    pub fn from_url_and_env(write_url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let write = pool::Config::from_url_and_env::<profiles::WritePool>(write_url)
            .map_err(FromUrlAndEnvError::Write)?;
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromUrlAndEnvError::MigrationsLock)?;

        Ok(Self {
            write,
            migrations_lock,
        })
    }
}
