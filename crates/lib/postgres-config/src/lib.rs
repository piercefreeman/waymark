//! Config for Postgres pools and connections.

#![warn(missing_docs)]

use std::num::NonZeroU64;

use waymark_nonzero_duration::NonZeroDuration;

pub mod connection;
pub mod pool;

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
