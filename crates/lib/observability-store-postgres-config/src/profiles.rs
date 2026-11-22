//! A Postgres observability store's profiles: where the config of its
//! pools and connections comes from.

use std::num::{NonZeroU32, NonZeroU64};

use waymark_nonzero_duration::NonZeroDuration;
use waymark_postgres_config::{connection, migrations_lock, pool};

/// The write pool: `WAYMARK_OBSERVABILITY_POSTGRES_*`.
#[derive(Debug)]
pub(crate) struct WritePool;

impl pool::Profile for WritePool {
    const PREFIX: &'static str = "OBSERVABILITY_POSTGRES";

    const MAX_CONNECTIONS_DEFAULT: NonZeroU32 = NonZeroU32::new(4).unwrap();

    // TODO: measure the longest legitimate write (a sink flush, a chunk of
    // a retention sweep) and size this from it.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(30);

    const ACQUIRE_TIMEOUT_DEFAULT: NonZeroDuration = pool::Config::DEFAULT_ACQUIRE_TIMEOUT;
}

/// The read pool: `WAYMARK_OBSERVABILITY_POSTGRES_READ_*`.
#[derive(Debug)]
pub(crate) struct ReadPool;

impl pool::Profile for ReadPool {
    const PREFIX: &'static str = "OBSERVABILITY_POSTGRES_READ";

    const MAX_CONNECTIONS_DEFAULT: NonZeroU32 = NonZeroU32::new(4).unwrap();

    // 10 seconds: sized for paged, range-bound reads; a read the client
    // gave up on holds its connection no longer than this.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(10);

    const ACQUIRE_TIMEOUT_DEFAULT: NonZeroDuration = pool::Config::DEFAULT_ACQUIRE_TIMEOUT;
}

/// The migrations connection: `WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_*`.
#[derive(Debug)]
pub(crate) struct Migrations;

impl connection::Profile for Migrations {
    const PREFIX: &'static str = "OBSERVABILITY_POSTGRES_MIGRATIONS";

    // 10 minutes: a migration on a large table is slow before it is
    // stuck.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(600);
}

/// The migrations lock: `WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_*`.
#[derive(Debug)]
pub(crate) struct MigrationsLock;

impl migrations_lock::Profile for MigrationsLock {
    const PREFIX: &'static str = "OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK";

    const HEARTBEAT_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(5).unwrap());

    const TIME_TO_LIVE_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(15).unwrap());

    const FENCING_MARGIN_DEFAULT: NonZeroDuration =
        NonZeroDuration::from_nonzero_secs(NonZeroU64::new(1).unwrap());

    // 3 seconds: a renewal that takes longer leaves room for another one
    // before the lock stops being relied on.
    const STATEMENT_TIMEOUT_DEFAULT: Option<NonZeroDuration> = NonZeroDuration::from_secs(3);
}
