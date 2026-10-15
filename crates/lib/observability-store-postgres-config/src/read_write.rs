//! A Postgres observability store that is written and read.

use waymark_secret_string::SecretString;

use waymark_postgres_config::{connection, pool};

use crate::profiles;

/// Configuration for a Postgres observability store that is written and
/// read.
#[derive(Debug, Clone)]
pub struct ReadWrite {
    /// The pool the sinks and the retention sweeps write through.
    pub write: pool::Config,

    /// The pool the query backends read through. Its URL may point at the
    /// same database as the write pool's, or at a read replica of it.
    pub read: pool::Config,

    /// The connection the migrations check runs through, to the write
    /// pool's URL; its statement timeout is the migrations connection's.
    pub migrations_check: connection::Config,
}

/// Error returned when reading a [`ReadWrite`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The write pool's config could not be read.
    #[error("write pool: {0}")]
    Write(#[source] pool::FromUrlAndEnvError),

    /// The read pool's config could not be read.
    #[error("read pool: {0}")]
    Read(#[source] pool::FromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] connection::FromUrlAndEnvError),
}

impl ReadWrite {
    /// Create config from environment variables, for the store written
    /// at `write_url` and read at `read_url`.
    pub fn from_urls_and_env(
        write_url: SecretString,
        read_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let write = pool::Config::from_url_and_env::<profiles::WritePool>(write_url.clone())
            .map_err(FromEnvError::Write)?;
        let read = pool::Config::from_url_and_env::<profiles::ReadPool>(read_url)
            .map_err(FromEnvError::Read)?;
        let migrations_check =
            connection::Config::from_url_and_env::<profiles::Migrations>(write_url)
                .map_err(FromEnvError::MigrationsCheck)?;

        Ok(Self {
            write,
            read,
            migrations_check,
        })
    }
}
