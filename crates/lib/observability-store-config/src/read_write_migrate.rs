//! An observability store that is written, read and migrated.

use waymark_secret_string::SecretString;

use crate::{Backend, UrlFromEnvError, backend, scheme};

/// An observability store that is written, read and migrated, dispatched
/// by URL scheme.
#[derive(Debug, Clone)]
pub enum ReadWriteMigrate {
    /// A Postgres store (`postgres://` / `postgresql://`).
    Postgres(waymark_observability_store_postgres_config::ReadWriteMigrate),
}

/// Error returned when reading a [`ReadWriteMigrate`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The database URL could not be read.
    #[error("database url: {0}")]
    DatabaseUrl(#[source] UrlFromEnvError),

    /// The read database URL could not be read.
    #[error("read database url: {0}")]
    ReadDatabaseUrl(#[source] UrlFromEnvError),

    /// The migrations database URL could not be read.
    #[error("migrations database url: {0}")]
    MigrationsDatabaseUrl(#[source] UrlFromEnvError),

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

    /// The migrations database URL is for a different backend than the
    /// database URL: the migrations must go to the store the writes go
    /// to.
    #[error("observability migrations database scheme {migrations:?} differs from {write:?}")]
    MigrationsSchemeDiffers {
        /// The scheme of the database URL.
        write: String,

        /// The scheme of the migrations database URL; empty when it had
        /// none.
        migrations: String,
    },

    /// The Postgres store's config could not be read.
    #[error("postgres: {0}")]
    Postgres(
        #[source] waymark_observability_store_postgres_config::read_write_migrate::FromEnvError,
    ),
}

impl ReadWriteMigrate {
    /// Create config from environment variables. `default_database_url`
    /// (the main database URL) is used when
    /// `WAYMARK_OBSERVABILITY_DATABASE_URL` is not set, and that URL in
    /// turn when `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is not set.
    /// When `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` is not set, the
    /// migrations go through `WAYMARK_OBSERVABILITY_DATABASE_URL`, or
    /// through `default_migrations_url` (the main database's migrations
    /// URL) when that is not set either.
    pub fn from_env_urls_with_default(
        default_database_url: &SecretString,
        default_migrations_url: &SecretString,
    ) -> Result<Self, FromEnvError> {
        let maybe_url: Option<SecretString> = envfury::maybe("WAYMARK_OBSERVABILITY_DATABASE_URL")
            .map_err(FromEnvError::DatabaseUrl)?;

        // A store in the main database is migrated with the main
        // database's migrations credentials.
        let migrations_url_default = match &maybe_url {
            Some(url) => url.clone(),
            None => default_migrations_url.clone(),
        };

        let url = maybe_url.unwrap_or_else(|| default_database_url.clone());
        let read_url =
            crate::read_url_from_env_with_default(&url).map_err(FromEnvError::ReadDatabaseUrl)?;
        let migrations_url = crate::migrations_url_from_env_with_default(&migrations_url_default)
            .map_err(FromEnvError::MigrationsDatabaseUrl)?;

        Self::from_urls_and_env(url, read_url, migrations_url)
    }

    /// Dispatch a database URL to its backend by scheme, with the URLs
    /// the backend reads and migrates through, reading the backend's own
    /// variables from the environment.
    pub fn from_urls_and_env(
        url: SecretString,
        read_url: SecretString,
        migrations_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let write_scheme = scheme(&url);
        let read_scheme = scheme(&read_url);
        let migrations_scheme = scheme(&migrations_url);

        let Some(write_backend) = backend(&write_scheme) else {
            return Err(FromEnvError::UnsupportedScheme {
                scheme: write_scheme,
            });
        };

        match (
            write_backend,
            backend(&read_scheme),
            backend(&migrations_scheme),
        ) {
            (Backend::Postgres, Some(Backend::Postgres), Some(Backend::Postgres)) => {
                let config =
                    waymark_observability_store_postgres_config::ReadWriteMigrate::from_urls_and_env(
                        url,
                        read_url,
                        migrations_url,
                    )
                    .map_err(FromEnvError::Postgres)?;
                Ok(Self::Postgres(config))
            }
            (Backend::Postgres, None, _) => Err(FromEnvError::ReadSchemeDiffers {
                write: write_scheme,
                read: read_scheme,
            }),
            (Backend::Postgres, Some(Backend::Postgres), None) => {
                Err(FromEnvError::MigrationsSchemeDiffers {
                    write: write_scheme,
                    migrations: migrations_scheme,
                })
            }
        }
    }
}
