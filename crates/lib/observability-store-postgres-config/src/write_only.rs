//! A Postgres observability store that is only written.

use waymark_secret_string::SecretString;

use waymark_postgres_config::{connection, pool};

use crate::profiles;

/// Configuration for a Postgres observability store that is only written.
#[derive(Debug, Clone)]
pub struct WriteOnly {
    /// The pool the sinks and the retention sweeps write through.
    pub write: pool::Config,

    /// The connection the migrations check runs through, to the write
    /// pool's URL; its statement timeout is the migrations connection's.
    pub migrations_check: connection::Config,
}

/// Error returned by [`WriteOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The write pool's config could not be read.
    #[error("write pool: {0}")]
    Write(#[source] pool::FromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] connection::FromUrlAndEnvError),
}

impl WriteOnly {
    /// Create config from environment variables, for the store written
    /// at `write_url`.
    pub fn from_url_and_env(write_url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let write = pool::Config::from_url_and_env::<profiles::WritePool>(write_url.clone())
            .map_err(FromUrlAndEnvError::Write)?;
        let migrations_check =
            connection::Config::from_url_and_env::<profiles::Migrations>(write_url)
                .map_err(FromUrlAndEnvError::MigrationsCheck)?;

        Ok(Self {
            write,
            migrations_check,
        })
    }
}
