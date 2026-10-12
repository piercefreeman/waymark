//! Config for Postgres pools.

use std::num::{NonZeroU32, NonZeroU64};

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

    /// How long taking a connection from the pool may wait before it
    /// fails.
    pub acquire_timeout: NonZeroDuration,
}

impl Config {
    /// The default acquire timeout: 30 seconds.
    pub const DEFAULT_ACQUIRE_TIMEOUT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(30).unwrap());
}
