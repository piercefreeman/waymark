//! The main database, read and written.

use waymark_postgres_config::{connection, pool};
use waymark_secret_string::SecretString;

use crate::profiles;
use crate::{DefaultsFor, MustUrlFromEnvError};

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
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] connection::FromUrlAndEnvError),
}

/// Error returned by [`ReadWriteOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] connection::FromUrlAndEnvError),
}

impl ReadWriteOnly {
    /// Create config from environment variables, the read-write pool's
    /// defaulted for `defaults_for`.
    pub fn from_env(defaults_for: DefaultsFor) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env().map_err(FromEnvError::DatabaseUrl)?;
        let read_write = defaults_for
            .read_write_pool_from_url_and_env(url.clone())
            .map_err(FromEnvError::ReadWrite)?;
        let migrations_check = connection::Config::from_url_and_env::<profiles::Migrations>(url)
            .map_err(FromEnvError::MigrationsCheck)?;

        Ok(Self {
            read_write,
            migrations_check,
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
            .read_write_pool_from_url_and_env(url.clone())
            .map_err(FromUrlAndEnvError::ReadWrite)?;
        let migrations_check = connection::Config::from_url_and_env::<profiles::Migrations>(url)
            .map_err(FromUrlAndEnvError::MigrationsCheck)?;

        Ok(Self {
            read_write,
            migrations_check,
        })
    }
}
