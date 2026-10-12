//! Config for Postgres connections.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

use crate::IntFromEnvError;

/// Configuration for one Postgres connection.
#[derive(Debug, Clone)]
pub struct Config {
    /// The database URL.
    pub url: SecretString,

    /// How long one statement may run before the server ends it; no
    /// limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}

/// Where a connection's config comes from: the prefix of its environment
/// variables and the defaults for the ones not set.
///
/// The variable is `WAYMARK_{PREFIX}_STATEMENT_TIMEOUT_MS` (`0` turns the
/// timeout off).
pub trait Profile {
    /// The variables' prefix, without the `WAYMARK_`.
    const PREFIX: &'static str;

    /// The statement timeout when its variable is not set; no limit when
    /// `None`.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration>;
}

/// Error returned by [`Config::from_url_and_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromUrlAndEnvError {
    /// The connection's statement timeout could not be read.
    #[error("statement timeout: {0}")]
    StatementTimeout(#[source] IntFromEnvError),
}

impl Config {
    /// Create config for the connection to `url`, with the rest from the
    /// environment variables of the profile `Profile`.
    pub fn from_url_and_env<Profile: self::Profile>(
        url: SecretString,
    ) -> Result<Self, FromUrlAndEnvError> {
        let statement_timeout = crate::statement_timeout_from_env(Profile::PREFIX)
            .map_err(FromUrlAndEnvError::StatementTimeout)?
            .unwrap_or(Profile::STATEMENT_TIMEOUT_DEFAULT);

        Ok(Self {
            url,
            statement_timeout,
        })
    }
}

#[cfg(test)]
mod tests;
