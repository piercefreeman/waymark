//! A Postgres observability store that is only read.

use waymark_secret_string::SecretString;

use waymark_postgres_config::pool;

use crate::profiles;

/// Configuration for a Postgres observability store that is only read.
#[derive(Debug, Clone)]
pub struct ReadOnly {
    /// The pool the query backends read through. Its URL may point at the
    /// store's database, or at a read replica of it.
    pub read: pool::Config,
}

impl ReadOnly {
    /// Create config from environment variables, for the store read at
    /// `read_url`.
    pub fn from_url_and_env(read_url: SecretString) -> Result<Self, pool::FromUrlAndEnvError> {
        let read = pool::Config::from_url_and_env::<profiles::ReadPool>(read_url)?;

        Ok(Self { read })
    }
}
