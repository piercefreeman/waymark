//! The main database, read and written.

use waymark_postgres_config::{migrations_lock, pool};
use waymark_secret_string::SecretString;

use crate::profiles;
use crate::{DefaultsFor, UrlFromEnvError};

/// Configuration for the main database, read and written.
#[derive(Debug, Clone)]
pub struct ReadWriteOnly {
    /// The pool the runtime reads and writes through.
    pub read_write: pool::Config,

    /// The shared migrations lock the runtime keeps, through the
    /// read-write pool's URL.
    pub migrations_lock: migrations_lock::Config,
}

/// Error returned by [`ReadWriteOnly::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
}

/// Error returned by [`ReadWriteOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
}

impl ReadWriteOnly {
    /// Create config from environment variables, the read-write pool's
    /// defaulted for `defaults_for`.
    pub fn from_env(defaults_for: DefaultsFor) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env().map_err(FromEnvError::DatabaseUrl)?;
        let read_write = defaults_for
            .read_write_pool_from_url_and_env(url)
            .map_err(FromEnvError::ReadWrite)?;
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromEnvError::MigrationsLock)?;

        Ok(Self {
            read_write,
            migrations_lock,
        })
    }

    /// Create config for the database at `url`, with the rest from
    /// environment variables, the read-write pool's defaulted for
    /// `defaults_for`.
    pub fn from_url_and_env(
        defaults_for: DefaultsFor,
        url: SecretString,
    ) -> Result<Self, FromUrlAndEnvError> {
        let read_write = defaults_for
            .read_write_pool_from_url_and_env(url)
            .map_err(FromUrlAndEnvError::ReadWrite)?;
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromUrlAndEnvError::MigrationsLock)?;

        Ok(Self {
            read_write,
            migrations_lock,
        })
    }
}
