//! The main database, read, written and migrated.

use waymark_postgres_config::{connection, migrations_lock, pool};
use waymark_secret_string::SecretString;

use crate::profiles;
use crate::{DefaultsFor, UrlFromEnvError};

/// Configuration for the main database, read, written and migrated.
#[derive(Debug, Clone)]
pub struct ReadWriteMigrate {
    /// The pool the runtime reads and writes through.
    pub read_write: pool::Config,

    /// The connection the migrations run through. Its URL may carry other
    /// credentials than the read-write pool's, with the rights the
    /// migrations need.
    pub migrations: connection::Config,

    /// The migrations lock: the exclusive one the migrations run under,
    /// through the migrations connection's URL, and the shared one the
    /// runtime keeps afterwards, through the read-write pool's URL.
    pub migrations_lock: migrations_lock::Config,
}

/// Error returned by [`ReadWriteMigrate::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// The migrations database URL could not be read.
    #[error("migrations database url: {0}")]
    MigrationsDatabaseUrl(#[source] UrlFromEnvError),

    /// The read-write pool's config could not be read.
    #[error("read-write pool: {0}")]
    ReadWrite(#[source] pool::FromUrlAndEnvError),

    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] connection::FromUrlAndEnvError),

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
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

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
}

impl ReadWriteMigrate {
    /// Create config from environment variables, the read-write pool's
    /// defaulted for `defaults_for`. The database is read and written at
    /// `WAYMARK_MAIN_DATABASE_URL`, or at the unified
    /// `WAYMARK_DATABASE_URL` when it is not set. Its migrations run
    /// through the first that is set of
    /// `WAYMARK_MAIN_MIGRATIONS_DATABASE_URL`, `WAYMARK_MAIN_DATABASE_URL`
    /// and the unified `WAYMARK_MIGRATIONS_DATABASE_URL` and
    /// `WAYMARK_DATABASE_URL`.
    pub fn from_env(defaults_for: DefaultsFor) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env().map_err(FromEnvError::DatabaseUrl)?;
        let migrations_url =
            crate::migrations_url_from_env().map_err(FromEnvError::MigrationsDatabaseUrl)?;
        let read_write = defaults_for
            .read_write_pool_from_url_and_env(url)
            .map_err(FromEnvError::ReadWrite)?;
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)
                .map_err(FromEnvError::Migrations)?;
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromEnvError::MigrationsLock)?;

        Ok(Self {
            read_write,
            migrations,
            migrations_lock,
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
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromUrlsAndEnvError::MigrationsLock)?;

        Ok(Self {
            read_write,
            migrations,
            migrations_lock,
        })
    }
}
