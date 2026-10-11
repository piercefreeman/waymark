//! An observability store that is only written.

use waymark_secret_string::SecretString;

use crate::{Backend, UrlFromEnvError, backend, scheme};

/// An observability store that is only written, dispatched by URL scheme.
#[derive(Debug, Clone)]
pub enum WriteOnly {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::WriteOnly),
}

/// Error returned when reading a [`WriteOnly`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// No supported backend uses the database URL's scheme.
    #[error("unsupported observability database scheme {scheme:?}")]
    UnsupportedScheme {
        /// The scheme found in the URL; empty when the URL had none.
        scheme: String,
    },

    /// The Postgres store's config could not be read.
    #[error("postgres: {0}")]
    Postgres(#[source] waymark_observability_store_postgres_config::write_only::FromEnvError),
}

impl WriteOnly {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set.
    pub fn from_env_url_with_default(
        default_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env_with_default(default_database_url)
            .map_err(FromEnvError::DatabaseUrl)?;

        Self::from_url_and_env(url)
    }

    /// Dispatch a database URL to its backend by scheme, reading the
    /// backend's own variables from the environment.
    pub fn from_url_and_env(url: SecretString) -> Result<Self, FromEnvError> {
        let scheme = scheme(&url);

        match backend(&scheme) {
            Some(Backend::Postgres) => {
                let config =
                    waymark_observability_store_postgres_config::WriteOnly::from_url_and_env(url)
                        .map_err(FromEnvError::Postgres)?;
                Ok(Self::Postgres(config))
            }
            None => Err(FromEnvError::UnsupportedScheme { scheme }),
        }
    }
}
