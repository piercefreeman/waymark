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
    /// The migrations database URL could not be told.
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
    Postgres(#[source] waymark_postgres_config::connection::FromUrlAndEnvError),
}

impl MigrateOnly {
    /// Create config from environment variables. The migrations go
    /// through the first that is set of
    /// `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL`,
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` and the unified
    /// `WAYMARK_MIGRATIONS_DATABASE_URL` and `WAYMARK_DATABASE_URL`.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let migrations_url =
            crate::migrations_url_from_env().map_err(FromEnvError::MigrationsDatabaseUrl)?;

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
