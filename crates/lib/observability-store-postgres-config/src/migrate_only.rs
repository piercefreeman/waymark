//! A Postgres observability store that is only migrated.

use waymark_secret_string::SecretString;

use waymark_postgres_config::{connection, migrations_lock};

use crate::profiles;

/// Configuration for a Postgres observability store that is only
/// migrated.
#[derive(Debug, Clone)]
pub struct MigrateOnly {
    /// The connection the migrations run through.
    pub migrations: connection::Config,

    /// The exclusive migrations lock the migrations run under, through
    /// the migrations connection's URL.
    pub migrations_lock: migrations_lock::Config,
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
    /// Create config from environment variables, for the store migrated
    /// at `migrations_url`.
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
