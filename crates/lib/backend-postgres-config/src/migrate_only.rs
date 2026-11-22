//! The main database, migrated.

use waymark_postgres_config::{connection, migrations_lock};
use waymark_secret_string::SecretString;

use crate::UrlFromEnvError;
use crate::profiles;

/// Configuration for the main database, migrated.
#[derive(Debug, Clone)]
pub struct MigrateOnly {
    /// The connection the migrations run through. Its URL may carry other
    /// credentials than the database URL's, with the rights the
    /// migrations need.
    pub migrations: connection::Config,

    /// The exclusive migrations lock the migrations run under, through
    /// the migrations connection's URL.
    pub migrations_lock: migrations_lock::Config,
}

/// Error returned by [`MigrateOnly::from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The migrations database URL could not be told.
    #[error("migrations database url: {0}")]
    MigrationsDatabaseUrl(#[source] UrlFromEnvError),

    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] connection::FromUrlAndEnvError),

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
}

/// Error returned by [`MigrateOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] connection::FromUrlAndEnvError),

    /// The migrations lock's config could not be read.
    #[error("migrations lock: {0}")]
    MigrationsLock(#[source] migrations_lock::FromEnvError),
}

impl MigrateOnly {
    /// Create config from environment variables. The migrations run
    /// through the first that is set of
    /// `WAYMARK_MAIN_MIGRATIONS_DATABASE_URL`, `WAYMARK_MAIN_DATABASE_URL`
    /// and the unified `WAYMARK_MIGRATIONS_DATABASE_URL` and
    /// `WAYMARK_DATABASE_URL`.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let migrations_url =
            crate::migrations_url_from_env().map_err(FromEnvError::MigrationsDatabaseUrl)?;
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)
                .map_err(FromEnvError::Migrations)?;
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromEnvError::MigrationsLock)?;

        Ok(Self {
            migrations,
            migrations_lock,
        })
    }

    /// Create config for the database migrated at `migrations_url`, with
    /// the rest from environment variables.
    pub fn from_url_and_env(migrations_url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)
                .map_err(FromUrlAndEnvError::Migrations)?;
        let migrations_lock = migrations_lock::Config::from_env::<profiles::MigrationsLock>()
            .map_err(FromUrlAndEnvError::MigrationsLock)?;

        Ok(Self {
            migrations,
            migrations_lock,
        })
    }
}
