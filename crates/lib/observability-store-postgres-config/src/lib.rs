//! Config for a Postgres observability store.
//!
//! The URLs are parameters rather than variables of their own: the
//! backend-neutral `WAYMARK_OBSERVABILITY_DATABASE_URL`,
//! `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` and
//! `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` are read and dispatched
//! by scheme in `waymark-observability-store-config`.

use std::num::NonZeroU32;

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

    /// How long one statement may run before the server ends it.
    pub statement_timeout: NonZeroDuration,
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
    /// statement the client gave up on keeps running otherwise, holding
    /// its connection; this is the hard end to it.
    pub statement_timeout: NonZeroDuration,
}

#[cfg(test)]
mod tests;
