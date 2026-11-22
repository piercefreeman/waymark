//! The migrations engine: what it does through a backend and with
//! migrations, whichever they are.

#![warn(missing_docs)]

pub mod migrations_lock_keeper;
pub mod migrations_lock_timing;

pub use self::migrations_lock_keeper::{
    ExclusiveMigrationsLock, MigrationsLockKeeper, SharedMigrationsLock,
};
pub use self::migrations_lock_timing::{MigrationsLockTiming, ValidMigrationsLockTiming};

#[cfg(test)]
mod test_helpers;
