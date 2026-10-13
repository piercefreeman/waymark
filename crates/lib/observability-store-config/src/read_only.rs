//! An observability store that is only read.

use waymark_secret_string::SecretString;

use crate::{Backend, UrlFromEnvError, backend, scheme};

/// An observability store that is only read, dispatched by URL scheme.
#[derive(Debug, Clone)]
pub enum ReadOnly {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::ReadOnly),
}

/// Error returned when reading a [`ReadOnly`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL, which the read database URL defaults to, could
    /// not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// The read database URL could not be read.
    #[error("read database url: {0}")]
    ReadDatabaseUrl(#[source] UrlFromEnvError),

    /// No supported backend uses the read database URL's scheme.
    #[error("unsupported observability read database scheme {scheme:?}")]
    UnsupportedScheme {
        /// The scheme found in the URL; empty when the URL had none.
        scheme: String,
    },

    /// The Postgres store's config could not be read.
    #[error("postgres: {0}")]
    Postgres(#[source] waymark_observability_store_postgres_config::read_only::FromEnvError),
}

impl ReadOnly {
    /// Create config from environment variables.
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is used when
    /// `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is not set, and
    /// `default_database_url` (the main database URL) in turn when that
    /// is not set.
    pub fn from_env_url_with_default(
        default_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env_with_default(default_database_url)
            .map_err(FromEnvError::DatabaseUrl)?;
        let read_url =
            crate::read_url_from_env_with_default(&url).map_err(FromEnvError::ReadDatabaseUrl)?;

        Self::from_url_and_env(read_url)
    }

    /// Dispatch the URL the store is read through to its backend by
    /// scheme, reading the backend's own variables from the environment.
    pub fn from_url_and_env(read_url: SecretString) -> Result<Self, FromEnvError> {
        let scheme = scheme(&read_url);

        match backend(&scheme) {
            Some(Backend::Postgres) => {
                let config =
                    waymark_observability_store_postgres_config::ReadOnly::from_url_and_env(
                        read_url,
                    )
                    .map_err(FromEnvError::Postgres)?;
                Ok(Self::Postgres(config))
            }
            None => Err(FromEnvError::UnsupportedScheme { scheme }),
        }
    }
}
