//! A Postgres observability store that is written, read and migrated.

use waymark_secret_string::SecretString;

use waymark_postgres_config::{connection, pool};

use crate::profiles;

/// Configuration for a Postgres observability store that is written,
/// read and migrated.
#[derive(Debug, Clone)]
pub struct ReadWriteMigrate {
    /// The pool the sinks and the retention sweeps write through.
    pub write: pool::Config,

    /// The pool the query backends read through. Its URL may point at the
    /// same database as the write pool's, or at a read replica of it.
    pub read: pool::Config,

    /// The connection the migrations run through. Its URL may carry other
    /// credentials than the write pool's, with the rights the migrations
    /// need.
    pub migrations: connection::Config,
}

/// Error returned when reading a [`ReadWriteMigrate`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The write pool's config could not be read.
    #[error("write pool: {0}")]
    Write(#[source] pool::FromUrlAndEnvError),

    /// The read pool's config could not be read.
    #[error("read pool: {0}")]
    Read(#[source] pool::FromUrlAndEnvError),

    /// The migrations connection's config could not be read.
    #[error("migrations connection: {0}")]
    Migrations(#[source] connection::FromUrlAndEnvError),
}

impl ReadWriteMigrate {
    /// Create config from environment variables, for the store written
    /// at `write_url`, read at `read_url` and migrated at
    /// `migrations_url`.
    pub fn from_urls_and_env(
        write_url: SecretString,
        read_url: SecretString,
        migrations_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let write = pool::Config::from_url_and_env::<profiles::WritePool>(write_url)
            .map_err(FromEnvError::Write)?;
        let read = pool::Config::from_url_and_env::<profiles::ReadPool>(read_url)
            .map_err(FromEnvError::Read)?;
        let migrations =
            connection::Config::from_url_and_env::<profiles::Migrations>(migrations_url)
                .map_err(FromEnvError::Migrations)?;

        Ok(Self {
            write,
            read,
            migrations,
        })
    }
}
