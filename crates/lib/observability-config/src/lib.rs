//! Config for observability: the observability database, and each
//! observability subsystem's config.

use waymark_secret_string::SecretString;

/// Configuration for observability.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// The observability database.
    pub db: waymark_observability_store_config::ReadWrite,

    /// The essential-metrics subsystem's config.
    pub essential_metrics: waymark_essential_metrics_config::EssentialMetricsConfig,

    /// The observability-events subsystem's config.
    pub observability_events: waymark_observability_events_config::ObservabilityEventsConfig,
}

/// Error returned when reading an [`ObservabilityConfig`] from the
/// environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The observability database's config could not be read.
    #[error("database: {0}")]
    Db(#[source] waymark_observability_store_config::read_write::FromEnvError),

    /// The essential-metrics subsystem's config could not be read.
    #[error("essential metrics: {0}")]
    EssentialMetrics(#[from] waymark_essential_metrics_config::FromEnvError),

    /// The observability-events subsystem's config could not be read.
    #[error("observability events: {0}")]
    ObservabilityEvents(#[source] waymark_observability_events_config::FromEnvError),
}

impl ObservabilityConfig {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set, and that URL in
    /// turn when `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is not set.
    pub fn from_env_urls_with_default(
        default_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let db = waymark_observability_store_config::ReadWrite::from_env_urls_with_default(
            default_database_url,
        )
        .map_err(FromEnvError::Db)?;
        Self::from_db_and_env(db)
    }

    /// Create config for the store written at `url` and read at
    /// `read_url`, with the rest from environment variables.
    ///
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` and
    /// `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` are not read: the URLs
    /// given stand in for them.
    pub fn from_urls_and_env(
        url: SecretString,
        read_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let db = waymark_observability_store_config::ReadWrite::from_urls_and_env(url, read_url)
            .map_err(FromEnvError::Db)?;
        Self::from_db_and_env(db)
    }

    /// Create config for the database `db`, with the subsystems' config
    /// from environment variables.
    fn from_db_and_env(
        db: waymark_observability_store_config::ReadWrite,
    ) -> Result<Self, FromEnvError> {
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
