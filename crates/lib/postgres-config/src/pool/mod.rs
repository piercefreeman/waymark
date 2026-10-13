//! Config for Postgres pools.

use std::num::{NonZeroU32, NonZeroU64};

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

use crate::IntFromEnvError;

/// Configuration for one Postgres pool.
#[derive(Debug, Clone)]
pub struct Config {
    /// The database URL.
    pub url: SecretString,

    /// Connection cap for the pool.
    pub max_connections: NonZeroU32,

    /// How long one statement may run before the server ends it. A
    /// statement the client gave up on keeps running otherwise, holding its
    /// connection; this is the hard end to it. No limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,

    /// How long taking a connection from the pool may wait before it
    /// fails.
    pub acquire_timeout: NonZeroDuration,
}

impl Config {
    /// The default acquire timeout: 30 seconds.
    pub const DEFAULT_ACQUIRE_TIMEOUT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(30).unwrap());
}

/// Where a pool's config comes from: the prefix of its environment
/// variables and the defaults for the ones not set.
///
/// The variables are `WAYMARK_{PREFIX}_MAX_CONNECTIONS`,
/// `WAYMARK_{PREFIX}_STATEMENT_TIMEOUT_MS` (`0` turns the timeout off) and
/// `WAYMARK_{PREFIX}_ACQUIRE_TIMEOUT_MS` (`0` is refused).
pub trait Profile {
    /// The variables' prefix, without the `WAYMARK_`.
    const PREFIX: &'static str;

    /// The connection cap when its variable is not set.
    const MAX_CONNECTIONS_DEFAULT: NonZeroU32;

    /// The statement timeout when its variable is not set; no limit when
    /// `None`.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration>;

    /// The acquire timeout when its variable is not set.
    const ACQUIRE_TIMEOUT_DEFAULT: NonZeroDuration;
}

/// Error returned by [`Config::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The pool's max-connections cap could not be read.
    #[error("max connections: {0}")]
    MaxConnections(#[source] IntFromEnvError),

    /// The pool's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] IntFromEnvError),

    /// The pool's acquire timeout could not be read.
    #[error("acquire timeout: {0}")]
    AcquireTimeout(#[source] IntFromEnvError),
}

impl Config {
    /// Create config for the pool connecting to `url`, with the rest from
    /// the environment variables of the profile `Profile`.
    pub fn from_url_and_env<Profile: self::Profile>(
        url: SecretString,
    ) -> Result<Self, FromUrlAndEnvError> {
        let max_connections =
            envfury::maybe(format!("WAYMARK_{}_MAX_CONNECTIONS", Profile::PREFIX))
                .map_err(FromUrlAndEnvError::MaxConnections)?
                .unwrap_or(Profile::MAX_CONNECTIONS_DEFAULT);
        let statement_timeout = crate::statement_timeout_from_env(Profile::PREFIX)
            .map_err(FromUrlAndEnvError::StatementTimeout)?
            .unwrap_or(Profile::STATEMENT_TIMEOUT_DEFAULT);
        let acquire_timeout =
            envfury::maybe(format!("WAYMARK_{}_ACQUIRE_TIMEOUT_MS", Profile::PREFIX))
                .map_err(FromUrlAndEnvError::AcquireTimeout)?
                .map_or(
                    Profile::ACQUIRE_TIMEOUT_DEFAULT,
                    NonZeroDuration::from_nonzero_millis,
                );

        Ok(Self {
            url,
            max_connections,
            statement_timeout,
            acquire_timeout,
        })
    }
}

#[cfg(test)]
mod tests;
