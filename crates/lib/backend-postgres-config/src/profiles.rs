//! The main database's profiles: where the config of its pools and
//! connections comes from.

use std::num::NonZeroU32;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_postgres_config::{connection, pool};

/// The executor's read-write pool: `WAYMARK_DATABASE_*`.
#[derive(Debug)]
pub(crate) struct ExecutorPool;

impl pool::Profile for ExecutorPool {
    const PREFIX: &'static str = "DATABASE";

    const MAX_CONNECTIONS_DEFAULT: NonZeroU32 = NonZeroU32::new(25).unwrap();

    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(60);

    const ACQUIRE_TIMEOUT_DEFAULT: NonZeroDuration = pool::Config::DEFAULT_ACQUIRE_TIMEOUT;
}

/// The bridge's read-write pool: `WAYMARK_DATABASE_*`, the executor's
/// variables, with the bridge's own defaults.
#[derive(Debug)]
pub(crate) struct BridgePool;

impl pool::Profile for BridgePool {
    const PREFIX: &'static str = "DATABASE";

    const MAX_CONNECTIONS_DEFAULT: NonZeroU32 = NonZeroU32::new(10).unwrap();

    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(60);

    const ACQUIRE_TIMEOUT_DEFAULT: NonZeroDuration = pool::Config::DEFAULT_ACQUIRE_TIMEOUT;
}

/// The migrations connection: `WAYMARK_MIGRATIONS_*`.
#[derive(Debug)]
pub(crate) struct Migrations;

impl connection::Profile for Migrations {
    const PREFIX: &'static str = "MIGRATIONS";

    // 10 minutes: a migration on a large table is slow before it is
    // stuck.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(600);
}
