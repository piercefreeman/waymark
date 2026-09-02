//! Config for the observability store: the backend-neutral database URL,
//! dispatched to a backend by its scheme.
//!
//! By default the observability store lives in the main database
//! (`WAYMARK_DATABASE_URL`) — same URL, namespace-separated into its own
//! schemas and accessed through its own pools. Pointing
//! `WAYMARK_OBSERVABILITY_DATABASE_URL` elsewhere moves it to a separate
//! server.

use waymark_secret_string::SecretString;

/// Configuration for observability.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// The observability database.
    pub db: Db,

    /// The essential-metrics family.
    pub essential_metrics: waymark_essential_metrics_config::EssentialMetricsConfig,
}

/// An observability database, dispatched by URL scheme.
#[derive(Debug, Clone)]
pub enum Db {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::PostgresConfig),
}

/// Error returned when reading an [`ObservabilityConfig`] from the
/// environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error(transparent)]
    DatabaseUrl(#[from] envfury::Error<envfury::ValueError<std::convert::Infallible>>),

    /// The database URL's scheme names no supported backend.
    #[error("unsupported observability database scheme {scheme:?}")]
    UnsupportedScheme {
        /// The scheme found in the URL; empty when the URL had none.
        scheme: String,
    },

    /// The Postgres store's config could not be read.
    #[error(transparent)]
    Postgres(#[from] waymark_observability_store_postgres_config::FromEnvError),

    /// The essential-metrics family's config could not be read.
    #[error(transparent)]
    EssentialMetrics(#[from] waymark_essential_metrics_config::FromEnvError),
}

impl ObservabilityConfig {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set.
    pub fn from_env(default_database_url: &SecretString) -> Result<Self, FromEnvError> {
        let url: SecretString = envfury::or_else("WAYMARK_OBSERVABILITY_DATABASE_URL", || {
            default_database_url.clone()
        })?;
        let db = Db::from_url(url)?;
        let essential_metrics =
            waymark_essential_metrics_config::EssentialMetricsConfig::from_env()?;
        Ok(Self {
            db,
            essential_metrics,
        })
    }
}

impl Db {
    /// Dispatch a database URL to its backend by scheme, reading the
    /// backend's own variables from the environment.
    pub fn from_url(url: SecretString) -> Result<Self, FromEnvError> {
        let scheme = url
            .expose_secret()
            .split_once("://")
            .map(|(scheme, _)| scheme.to_owned());
        match scheme.as_deref() {
            Some("postgres" | "postgresql") => Ok(Self::Postgres(
                waymark_observability_store_postgres_config::PostgresConfig::from_env(url)?,
            )),
            _ => Err(FromEnvError::UnsupportedScheme {
                scheme: scheme.unwrap_or_default(),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
