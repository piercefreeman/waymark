//! Config for Postgres pools and connections.

#![warn(missing_docs)]

use std::num::{NonZeroU32, NonZeroU64};

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

/// Configuration for one Postgres connection.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// The database URL.
    pub url: SecretString,

    /// How long one statement may run before the server ends it; no
    /// limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}

/// Configuration for one Postgres pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// The database URL.
    pub url: SecretString,

    /// Connection cap for the pool.
    pub max_connections: NonZeroU32,

    /// How long one statement may run before the server ends it. A
    /// statement the client gave up on keeps running otherwise, holding its
    /// connection; this is the hard end to it. No limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}

/// A statement timeout from the environment variable `name`, in
/// milliseconds, `default_millis` when it is not set: `0` turns the
/// timeout off.
pub fn statement_timeout_from_env(
    name: &'static str,
    default_millis: &'static str,
) -> Result<Option<NonZeroDuration>, envfury::Error<envfury::OrParseError<std::num::ParseIntError>>>
{
    let millis: u64 = envfury::or_parse(name, default_millis)?;

    Ok(statement_timeout_from_millis(millis))
}

/// A statement timeout of `millis` milliseconds: `0` turns the timeout off.
fn statement_timeout_from_millis(millis: u64) -> Option<NonZeroDuration> {
    NonZeroU64::new(millis).map(NonZeroDuration::from_nonzero_millis)
}

#[cfg(test)]
mod tests;
