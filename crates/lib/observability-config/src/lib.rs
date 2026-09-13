//! Config for the observability store: the backend-neutral database URL,
//! dispatched to a backend by its scheme.
//!
//! By default the observability store lives in the main database
//! (`WAYMARK_DATABASE_URL`) — same URL, namespace-separated into its own
//! schemas and accessed through its own pools. Pointing
//! `WAYMARK_OBSERVABILITY_DATABASE_URL` elsewhere moves it to a separate
//! server; `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is where the reads
//! go — the same database by default, or a replica of it.

use waymark_secret_string::SecretString;

/// Configuration for observability.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// The observability database.
    pub db: Db,

    /// The essential-metrics subsystem's config.
    pub essential_metrics: waymark_essential_metrics_config::EssentialMetricsConfig,

    /// The observability-events subsystem's config.
    pub observability_events: waymark_observability_events_config::ObservabilityEventsConfig,
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
    DatabaseUrl(envfury::Error<envfury::ValueError<std::convert::Infallible>>),

    /// The read database URL could not be read.
    #[error(transparent)]
    ReadDatabaseUrl(envfury::Error<envfury::ValueError<std::convert::Infallible>>),

    /// The database URL's scheme names no supported backend.
    #[error("unsupported observability database scheme {scheme:?}")]
    UnsupportedScheme {
        /// The scheme found in the URL; empty when the URL had none.
        scheme: String,
    },

    /// The read database URL names a different backend than the
    /// database URL: the reads must go to the store the writes go to.
    #[error("observability read database scheme {read:?} differs from {write:?}")]
    ReadSchemeDiffers {
        /// The scheme of the database URL.
        write: String,

        /// The scheme of the read database URL; empty when it had none.
        read: String,
    },

    /// The Postgres store's config could not be read.
    #[error(transparent)]
    Postgres(#[from] waymark_observability_store_postgres_config::FromEnvError),

    /// The essential-metrics subsystem's config could not be read.
    #[error(transparent)]
    EssentialMetrics(#[from] waymark_essential_metrics_config::FromEnvError),

    /// The observability-events subsystem's config could not be read.
    #[error(transparent)]
    ObservabilityEvents(waymark_observability_events_config::FromEnvError),
}

impl ObservabilityConfig {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set, and that URL in
    /// turn when `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is not set.
    pub fn from_env(default_database_url: &SecretString) -> Result<Self, FromEnvError> {
        let url: SecretString = envfury::or_else("WAYMARK_OBSERVABILITY_DATABASE_URL", || {
            default_database_url.clone()
        })
        .map_err(FromEnvError::DatabaseUrl)?;
        let read_url: SecretString =
            envfury::or_else("WAYMARK_OBSERVABILITY_READ_DATABASE_URL", || url.clone())
                .map_err(FromEnvError::ReadDatabaseUrl)?;
        let db = Db::from_urls(url, read_url)?;
        let essential_metrics =
            waymark_essential_metrics_config::EssentialMetricsConfig::from_env()?;
        let observability_events =
            waymark_observability_events_config::ObservabilityEventsConfig::from_env()
                .map_err(FromEnvError::ObservabilityEvents)?;
        Ok(Self {
            db,
            essential_metrics,
            observability_events,
        })
    }
}

/// The scheme of a URL; empty when it has none.
fn scheme(url: &SecretString) -> String {
    url.expose_secret()
        .split_once("://")
        .map(|(scheme, _)| scheme.to_owned())
        .unwrap_or_default()
}

impl Db {
    /// Dispatch a database URL to its backend by scheme, with the URL
    /// the backend reads through, reading the backend's own variables
    /// from the environment.
    pub fn from_urls(url: SecretString, read_url: SecretString) -> Result<Self, FromEnvError> {
        let write_scheme = scheme(&url);
        let read_scheme = scheme(&read_url);
        if read_scheme != write_scheme {
            return Err(FromEnvError::ReadSchemeDiffers {
                write: write_scheme,
                read: read_scheme,
            });
        }
        match write_scheme.as_str() {
            "postgres" | "postgresql" => Ok(Self::Postgres(
                waymark_observability_store_postgres_config::PostgresConfig::from_env(
                    url, read_url,
                )?,
            )),
            _ => Err(FromEnvError::UnsupportedScheme {
                scheme: write_scheme,
            }),
        }
    }
}

#[cfg(test)]
mod tests;
