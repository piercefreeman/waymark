//! Config for Postgres connections.

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

/// Configuration for one Postgres connection.
#[derive(Debug, Clone)]
pub struct Config {
    /// The database URL.
    pub url: SecretString,

    /// How long one statement may run before the server ends it; no
    /// limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}
