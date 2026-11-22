//! The backend interface of the migrations engine: the traits a database
//! backend implements.
//!
//! Every operation acts on the one connection its implementor holds.

#![warn(missing_docs)]

mod common;

pub mod access_migrations_connection;
pub mod begin_whole_run_transaction;
pub mod commit_whole_run_transaction;
pub mod convert_migrations_lock_to_shared;
pub mod find_exclusive_migrations_lock;
pub mod mark_migration_finished;
pub mod mark_migration_started;
pub mod migrations_ledger;
pub mod migrations_lock;
pub mod read_migrations_ledger;
pub mod release_migrations_lock;
pub mod renew_migrations_lock;
pub mod rollback_whole_run_transaction;
pub mod setup_migrations_engine;
pub mod take_exclusive_migrations_lock;
pub mod take_shared_migrations_lock;

pub use self::access_migrations_connection::AccessMigrationsConnection;
pub use self::begin_whole_run_transaction::BeginWholeRunTransaction;
pub use self::commit_whole_run_transaction::CommitWholeRunTransaction;
pub use self::common::*;
pub use self::convert_migrations_lock_to_shared::ConvertMigrationsLockToShared;
pub use self::find_exclusive_migrations_lock::FindExclusiveMigrationsLock;
pub use self::mark_migration_finished::MarkMigrationFinished;
pub use self::mark_migration_started::MarkMigrationStarted;
pub use self::migrations_ledger::MigrationsLedger;
pub use self::read_migrations_ledger::ReadMigrationsLedger;
pub use self::release_migrations_lock::ReleaseMigrationsLock;
pub use self::renew_migrations_lock::RenewMigrationsLock;
pub use self::rollback_whole_run_transaction::RollbackWholeRunTransaction;
pub use self::setup_migrations_engine::SetupMigrationsEngine;
pub use self::take_exclusive_migrations_lock::TakeExclusiveMigrationsLock;
pub use self::take_shared_migrations_lock::TakeSharedMigrationsLock;

/// A backend of the migrations engine.
///
/// Blanket-implemented for any type that implements every trait of the
/// backend interface.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Backend:
    AccessMigrationsConnection
    + SetupMigrationsEngine
    + ReadMigrationsLedger
    + MarkMigrationStarted
    + MarkMigrationFinished
    + BeginWholeRunTransaction
    + CommitWholeRunTransaction
    + RollbackWholeRunTransaction
    + TakeSharedMigrationsLock
    + TakeExclusiveMigrationsLock
    + ConvertMigrationsLockToShared
    + RenewMigrationsLock
    + ReleaseMigrationsLock
    + FindExclusiveMigrationsLock
{
}
