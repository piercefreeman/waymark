//! Config for observability: the observability database, and each
//! observability subsystem's config.

use waymark_secret_string::SecretString;

/// Configuration for observability.
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// The observability database.
    pub db: waymark_observability_store_config::ReadWriteMigrate,

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
    Db(#[source] waymark_observability_store_config::read_write_migrate::FromEnvError),

    /// The essential-metrics subsystem's config could not be read.
    #[error("essential metrics: {0}")]
    EssentialMetrics(#[from] waymark_essential_metrics_config::FromEnvError),

    /// The observability-events subsystem's config could not be read.
    #[error("observability events: {0}")]
    ObservabilityEvents(#[source] waymark_observability_events_config::FromEnvError),
}

impl ObservabilityConfig {
    /// Create config from environment variables. The store's URLs come
    /// from its own variables, with the unified `WAYMARK_DATABASE_URL` and
    /// `WAYMARK_MIGRATIONS_DATABASE_URL` standing in for the ones that are
    /// not set, as
    /// [`waymark_observability_store_config::ReadWriteMigrate::from_env`]
    /// has it.
    pub fn from_env() -> Result<Self, FromEnvError> {
        let db = waymark_observability_store_config::ReadWriteMigrate::from_env()
            .map_err(FromEnvError::Db)?;
        Self::from_db_and_env(db)
    }

    /// Create config from environment variables, for a caller that was
    /// given the unified URLs: `unified_database_url` and
    /// `unified_migrations_database_url` stand in for
    /// `WAYMARK_DATABASE_URL` and `WAYMARK_MIGRATIONS_DATABASE_URL`, which
    /// are not read.
    pub fn from_env_with_unified_urls(
        unified_database_url: &SecretString,
        unified_migrations_database_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let db = waymark_observability_store_config::ReadWriteMigrate::from_env_with_unified_urls(
            unified_database_url,
            unified_migrations_database_url,
        )
        .map_err(FromEnvError::Db)?;
        Self::from_db_and_env(db)
    }

    /// Create config for the store written at `url`, read at `read_url`
    /// and migrated at `migrations_url`, with the rest from environment
    /// variables.
    ///
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL`,
    /// `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` and
    /// `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` are not read: the
    /// URLs given stand in for them.
    pub fn from_urls_and_env(
        url: SecretString,
        read_url: SecretString,
        migrations_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let db = waymark_observability_store_config::ReadWriteMigrate::from_urls_and_env(
            url,
            read_url,
            migrations_url,
        )
        .map_err(FromEnvError::Db)?;
        Self::from_db_and_env(db)
    }

    /// Create config for the database `db`, with the subsystems' config
    /// from environment variables.
    fn from_db_and_env(
        db: waymark_observability_store_config::ReadWriteMigrate,
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
