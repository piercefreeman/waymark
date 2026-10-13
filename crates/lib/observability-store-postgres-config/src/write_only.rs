//! A Postgres observability store that is only written.

use waymark_secret_string::SecretString;

use waymark_postgres_config::pool;

use crate::profiles;

/// Configuration for a Postgres observability store that is only written.
#[derive(Debug, Clone)]
pub struct WriteOnly {
    /// The pool the sinks and the retention sweeps write through.
    pub write: pool::Config,
}

impl WriteOnly {
    /// Create config from environment variables, for the store written
    /// at `write_url`.
    pub fn from_url_and_env(write_url: SecretString) -> Result<Self, pool::FromUrlAndEnvError> {
        let write = pool::Config::from_url_and_env::<profiles::WritePool>(write_url)?;

        Ok(Self { write })
    }
}
