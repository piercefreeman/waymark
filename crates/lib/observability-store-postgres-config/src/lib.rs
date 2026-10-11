//! Config for a Postgres observability store.
//!
//! The URLs are parameters rather than variables of their own: the
//! backend-neutral `WAYMARK_OBSERVABILITY_DATABASE_URL`,
//! `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` and
//! `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` are read and dispatched
//! by scheme in `waymark-observability-store-config`.

use std::num::{NonZeroU32, NonZeroU64};

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

pub mod migrate_only;
pub mod read_only;
pub mod read_write;
pub mod read_write_migrate;
pub mod write_only;

pub use self::migrate_only::MigrateOnly;
pub use self::read_only::ReadOnly;
pub use self::read_write::ReadWrite;
pub use self::read_write_migrate::ReadWriteMigrate;
pub use self::write_only::WriteOnly;

/// Configuration for one connection to a Postgres observability store.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// The database URL.
    pub url: SecretString,

    /// How long one statement may run before the server ends it; no
    /// limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}

/// Configuration for one pool of a Postgres observability store.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// The database URL.
    pub url: SecretString,

    /// Connection cap for the pool. Observability pools are always the
    /// consumer's own — never the main database pool — so a slow
    /// observability statement can only ever wait on this budget, and a
    /// runaway read can only ever exhaust the read pool.
    ///
    /// Note for sizing: when the observability store shares the main
    /// database (the default), these connections are ADDITIVE — the
    /// server sees the main pool's connections plus the cap of every
    /// observability pool, so the main pool's own cap understates the
    /// total.
    pub max_connections: NonZeroU32,

    /// How long one statement may run before the server ends it. A
    /// statement the client gave up on keeps running otherwise, holding its
    /// connection; this is the hard end to it. No limit when `None`.
    pub statement_timeout: Option<NonZeroDuration>,
}

/// A statement timeout from the environment variable `name`, in
/// milliseconds, `default_millis` when it is not set: `0` turns the
/// timeout off.
pub(crate) fn statement_timeout_from_env(
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
