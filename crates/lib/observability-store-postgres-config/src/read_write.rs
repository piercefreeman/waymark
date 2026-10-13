//! A Postgres observability store that is written and read.

use waymark_secret_string::SecretString;

use waymark_postgres_config::PoolConfig;

/// Configuration for a Postgres observability store that is written and
/// read.
#[derive(Debug, Clone)]
pub struct ReadWrite {
    /// The pool the sinks and the retention sweeps write through.
    pub write: PoolConfig,

    /// The pool the query backends read through. Its URL may point at the
    /// same database as the write pool's, or at a read replica of it.
    pub read: PoolConfig,
}

/// Error returned when reading a [`ReadWrite`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// The write pool's config could not be read.
    #[error("write pool: {0}")]
    Write(#[source] crate::write_only::FromEnvError),

    /// The read pool's config could not be read.
    #[error("read pool: {0}")]
    Read(#[source] crate::read_only::FromEnvError),
}

impl ReadWrite {
    /// Create config from environment variables, for the store written
    /// at `write_url` and read at `read_url`.
    pub fn from_urls_and_env(
        write_url: SecretString,
        read_url: SecretString,
    ) -> Result<Self, FromEnvError> {
        let write =
            crate::write_only::pool_from_url_and_env(write_url).map_err(FromEnvError::Write)?;
        let read = crate::read_only::pool_from_url_and_env(read_url).map_err(FromEnvError::Read)?;

        Ok(Self { write, read })
    }
}
