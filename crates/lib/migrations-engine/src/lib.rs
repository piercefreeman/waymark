//! The migrations engine: what it does through a backend and with
//! migrations, whichever they are.

#![warn(missing_docs)]

pub mod check_migrations;
pub mod migrations_comparison;
pub mod migrations_engine_assertions;
pub mod migrations_lock_keeper;
pub mod migrations_lock_timing;
pub mod migrations_status;
pub mod run_migrations;

pub use self::check_migrations::check_migrations;
pub use self::migrations_comparison::{
    MigrationsComparison, ValidMigrationsArray, validate_migrations_array,
};
pub use self::migrations_lock_keeper::{
    ExclusiveMigrationsLock, MigrationsLockKeeper, SharedMigrationsLock,
};
pub use self::migrations_lock_timing::{MigrationsLockTiming, ValidMigrationsLockTiming};
pub use self::migrations_status::migrations_status;
pub use self::run_migrations::{MigrationsRunTransactions, run_migrations};

#[cfg(test)]
mod test_helpers;
