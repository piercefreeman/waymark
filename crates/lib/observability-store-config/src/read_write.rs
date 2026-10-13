//! An observability store that is written and read.

use waymark_secret_string::SecretString;

use crate::{Backend, UrlFromEnvError, backend, scheme};

/// An observability store that is written and read, dispatched by URL
/// scheme.
#[derive(Debug, Clone)]
pub enum ReadWrite {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::ReadWrite),
}

/// Error returned when reading a [`ReadWrite`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// The read database URL could not be read.
    #[error("read database url: {0}")]
    ReadDatabaseUrl(#[source] UrlFromEnvError),

    /// No supported backend uses the database URL's scheme.
    #[error("unsupported observability database scheme {scheme:?}")]
    UnsupportedScheme {
        /// The scheme found in the URL; empty when the URL had none.
        scheme: String,
    },

    /// The read database URL is for a different backend than the
    /// database URL: the reads must go to the store the writes go to.
    #[error("observability read database scheme {read:?} differs from {write:?}")]
    ReadSchemeDiffers {
        /// The scheme of the database URL.
        write: String,

        /// The scheme of the read database URL; empty when it had none.
        read: String,
    },

    /// The Postgres store's config could not be read.
    #[error("postgres: {0}")]
    Postgres(#[source] waymark_observability_store_postgres_config::read_write::FromEnvError),
}

impl ReadWrite {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set, and that URL in
    /// turn when `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is not set.
    pub fn from_env_urls_with_default(
        default_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let url = crate::url_from_env_with_default(default_database_url)
            .map_err(FromEnvError::DatabaseUrl)?;
        let read_url =
            crate::read_url_from_env_with_default(&url).map_err(FromEnvError::ReadDatabaseUrl)?;

        Self::from_urls_and_env(url, read_url)
    }

    /// Dispatch a database URL to its backend by scheme, with the URL
    /// the backend reads through, reading the backend's own variables
    /// from the environment.
    pub fn from_urls_and_env(
        url: SecretString,
        read_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let write_scheme = scheme(&url);
        let read_scheme = scheme(&read_url);

        let Some(write_backend) = backend(&write_scheme) else {
            return Err(FromEnvError::UnsupportedScheme {
                scheme: write_scheme,
            });
        };

        match (write_backend, backend(&read_scheme)) {
            (Backend::Postgres, Some(Backend::Postgres)) => {
                let config =
                    waymark_observability_store_postgres_config::ReadWrite::from_urls_and_env(
                        url, read_url,
                    )
                    .map_err(FromEnvError::Postgres)?;
                Ok(Self::Postgres(config))
            }
            (Backend::Postgres, None) => Err(FromEnvError::ReadSchemeDiffers {
                write: write_scheme,
                read: read_scheme,
            }),
        }
    }
}
