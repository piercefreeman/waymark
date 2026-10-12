//! Config for Postgres pools.

use std::num::NonZeroU32;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

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
}
