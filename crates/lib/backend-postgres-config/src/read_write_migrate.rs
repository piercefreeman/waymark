//! The main database, read, written and migrated.

use waymark_postgres_config::{connection, pool};
use waymark_secret_string::SecretString;

use crate::profiles;
use crate::{DefaultsFor, MustUrlFromEnvError, UrlFromEnvError};

/// Configuration for the main database, read, written and migrated.
#[derive(Debug, Clone)]
pub struct ReadWriteMigrate {
    /// The pool the runtime reads and writes through.
    pub read_write: pool::Config,

    /// The connection the migrations run through. Its URL may carry other
    /// credentials than the read-write pool's, with the rights the
    /// migrations need.
    pub migrations: connection::Config,
}

/// Error returned by [`ReadWriteMigrate::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] MustUrlFromEnvError),

    /// The migrations database URL could not be read.
    #[error("migrations database url: {0}")]
    MigrationsDatabaseUrl(#[source] UrlFromEnvError),

    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] connection::FromUrlAndEnvError),
}

/// Error returned by [`ReadWriteMigrate::from_urls_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlsAndEnvError {
    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] connection::FromUrlAndEnvError),
}

impl ReadWriteMigrate {
    /// Create config from environment variables, the read-write pool's
    /// defaulted for `defaults_for`. `WAYMARK_DATABASE_URL` is used when
    /// `WAYMARK_MIGRATIONS_DATABASE_URL` is not set.
    pub fn from_env(defaults_for: DefaultsFor) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env().map_err(FromEnvError::DatabaseUrl)?;
        let migrations_url = crate::migrations_url_from_env_with_default(&url)
            .map_err(FromEnvError::MigrationsDatabaseUrl)?;
        let read_write = defaults_for
            .read_write_pool_from_url_and_env(url)
            .map_err(FromEnvError::ReadWrite)?;
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)
                .map_err(FromEnvError::Migrations)?;

        Ok(Self {
            read_write,
            migrations,
        })
    }

    /// Create config for the database read and written at `url` and
    /// migrated at `migrations_url`, with the rest from environment
    /// variables, the read-write pool's defaulted for `defaults_for`.
    pub fn from_urls_and_env(
        defaults_for: DefaultsFor,
        url: SecretString,
        migrations_url: SecretString,
    ) -> Result<Self, FromUrlsAndEnvError> {
        let read_write = defaults_for
            .read_write_pool_from_url_and_env(url)
            .map_err(FromUrlsAndEnvError::ReadWrite)?;
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)
                .map_err(FromUrlsAndEnvError::Migrations)?;

        Ok(Self {
            read_write,
            migrations,
        })
    }
}
