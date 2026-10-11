//! Config for the observability store: the backend-neutral database URL,
//! dispatched to a backend by its scheme.
//!
//! By default the observability store lives in the main database
//! (`WAYMARK_DATABASE_URL`) — same URL, namespace-separated into its own
//! schema and accessed through its own pools. Pointing
//! `WAYMARK_OBSERVABILITY_DATABASE_URL` elsewhere moves it to a separate
//! server; `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is where the reads
//! go — the same database by default, or a replica of it.

#![warn(missing_docs)]

use waymark_secret_string::SecretString;

/// An observability database, dispatched by URL scheme.
#[derive(Debug, Clone)]
pub enum Db {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::PostgresConfig),
}

/// Error returned when reading a [`Db`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] envfury::Error<envfury::ValueError<std::convert::Infallible>>),

    /// The read database URL could not be read.
    #[error("read database url: {0}")]
    ReadDatabaseUrl(#[source] envfury::Error<envfury::ValueError<std::convert::Infallible>>),

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
    Postgres(#[from] waymark_observability_store_postgres_config::FromEnvError),
}

/// The scheme of a URL; empty when it has none.
fn scheme(url: &SecretString) -> String {
    url.expose_secret()
        .split_once("://")
        .map(|(scheme, _)| scheme.to_owned())
        .unwrap_or_default()
}

impl Db {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set, and that URL in
    /// turn when `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is not set.
    pub fn from_env_urls_with_default(
        default_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let url: SecretString = envfury::or_else("WAYMARK_OBSERVABILITY_DATABASE_URL", || {
            default_database_url.clone()
        })
        .map_err(FromEnvError::DatabaseUrl)?;
        let read_url: SecretString =
            envfury::or_else("WAYMARK_OBSERVABILITY_READ_DATABASE_URL", || url.clone())
                .map_err(FromEnvError::ReadDatabaseUrl)?;
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
        match write_scheme.as_str() {
            "postgres" | "postgresql" => {
                // The backend is what must agree, not the spelling.
                if !matches!(read_scheme.as_str(), "postgres" | "postgresql") {
                    return Err(FromEnvError::ReadSchemeDiffers {
                        write: write_scheme,
                        read: read_scheme,
                    });
                }
                let config =
                    waymark_observability_store_postgres_config::PostgresConfig::from_urls_and_env(
                        url, read_url,
                    )?;
                Ok(Self::Postgres(config))
            }
            _ => Err(FromEnvError::UnsupportedScheme {
                scheme: write_scheme,
            }),
        }
    }
}

#[cfg(test)]
mod tests;
