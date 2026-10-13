//! Config for Postgres pools and connections.

#![warn(missing_docs)]

use std::num::NonZeroU64;

use waymark_nonzero_duration::NonZeroDuration;

pub mod connection;
pub mod pool;

/// The error of reading a number from the environment.
pub type IntFromEnvError = envfury::Error<envfury::ValueError<std::num::ParseIntError>>;

/// The statement timeout from `WAYMARK_{prefix}_STATEMENT_TIMEOUT_MS`, in
/// milliseconds: `None` when the variable is not set, `Some(None)` when it
/// is `0`, which turns the timeout off.
fn statement_timeout_from_env(
    prefix: &str,
) -> Result<Option<Option<NonZeroDuration>>, IntFromEnvError> {
    let millis: Option<u64> = envfury::maybe(format!("WAYMARK_{prefix}_STATEMENT_TIMEOUT_MS"))?;

    Ok(millis.map(statement_timeout_from_millis))
}

/// A statement timeout of `millis` milliseconds: `0` turns the timeout off.
fn statement_timeout_from_millis(millis: u64) -> Option<NonZeroDuration> {
    NonZeroU64::new(millis).map(NonZeroDuration::from_nonzero_millis)
}

#[cfg(test)]
mod tests;
