//! A Postgres observability store that is only read.

use waymark_secret_string::SecretString;

use waymark_postgres_config::{connection, pool};

use crate::profiles;

/// Configuration for a Postgres observability store that is only read.
#[derive(Debug, Clone)]
pub struct ReadOnly {
    /// The pool the query backends read through. Its URL may point at the
    /// store's database, or at a read replica of it.
    pub read: pool::Config,

    /// The connection the migrations check runs through, to the read
    /// pool's URL, which may be a read replica's: a read-only store has no
    /// primary to check instead. Its statement timeout is the migrations
    /// connection's.
    pub migrations_check: connection::Config,
}

/// Error returned by [`ReadOnly::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The read pool's config could not be read.
    #[error("read pool: {0}")]
    Read(#[source] pool::FromUrlAndEnvError),

    /// The migrations check connection's config could not be read.
    #[error("migrations check connection: {0}")]
    MigrationsCheck(#[source] connection::FromUrlAndEnvError),
}

impl ReadOnly {
    /// Create config from environment variables, for the store read at
    /// `read_url`.
    pub fn from_url_and_env(read_url: SecretString) -> Result<Self, FromUrlAndEnvError> {
        let read = pool::Config::from_url_and_env::<profiles::ReadPool>(read_url.clone())
            .map_err(FromUrlAndEnvError::Read)?;
        let migrations_check =
            connection::Config::from_url_and_env::<profiles::Migrations>(read_url)
                .map_err(FromUrlAndEnvError::MigrationsCheck)?;

        Ok(Self {
            read,
            migrations_check,
        })
    }
}
