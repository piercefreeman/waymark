//! An observability store that is only migrated.

use waymark_secret_string::SecretString;

use crate::{Backend, UrlFromEnvError, backend, scheme};

/// An observability store that is only migrated, dispatched by URL scheme.
#[derive(Debug, Clone)]
pub enum MigrateOnly {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::MigrateOnly),
}

/// Error returned when reading a [`MigrateOnly`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL, which the migrations database URL defaults to,
    /// could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// The migrations database URL could not be read.
    #[error("migrations database url: {0}")]
    MigrationsDatabaseUrl(#[source] UrlFromEnvError),

    /// No supported backend uses the migrations database URL's scheme.
    #[error("unsupported observability migrations database scheme {scheme:?}")]
    UnsupportedScheme {
        /// The scheme found in the URL; empty when the URL had none.
        scheme: String,
    },

    /// The Postgres store's config could not be read.
    #[error("postgres: {0}")]
    Postgres(#[source] waymark_observability_store_postgres_config::migrate_only::FromEnvError),
}

impl MigrateOnly {
    /// Create config from environment variables.
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is used when
    /// `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` is not set, and
    /// `default_database_url` (the main database URL) in turn when that
    /// is not set.
    pub fn from_env_url_with_default(
        default_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env_with_default(default_database_url)
            .map_err(FromEnvError::DatabaseUrl)?;
        let migrations_url = crate::migrations_url_from_env_with_default(&url)
            .map_err(FromEnvError::MigrationsDatabaseUrl)?;

        Self::from_url_and_env(migrations_url)
    }

    /// Dispatch the URL the store is migrated through to its backend by
    /// scheme, reading the backend's own variables from the environment.
    pub fn from_url_and_env(migrations_url: SecretString) -> Result<Self, FromEnvError> {
        let scheme = scheme(&migrations_url);

        match backend(&scheme) {
            Some(Backend::Postgres) => {
                let config =
                    waymark_observability_store_postgres_config::MigrateOnly::from_url_and_env(
                        migrations_url,
                    )
                    .map_err(FromEnvError::Postgres)?;
                Ok(Self::Postgres(config))
            }
            None => Err(FromEnvError::UnsupportedScheme { scheme }),
        }
    }
}
