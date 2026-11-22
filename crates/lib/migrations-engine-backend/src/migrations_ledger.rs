//! The migrations ledger of a database.

use waymark_migrations_engine_migration::{MigrationRevision, MigrationVersion};

/// The migrations ledger of a database.
#[derive(Debug)]
pub enum MigrationsLedger {
    /// The migrations engine is not set up in the database: it has no
    /// ledger.
    NotSetUp,

    /// The entries of the ledger, in no particular order.
    SetUp(Vec<Entry>),
}

/// An entry of the migrations ledger: one migration the database has
/// seen.
#[derive(Debug)]
pub struct Entry {
    /// The version of the migration.
    pub version: MigrationVersion,

    /// The revision of the migration, as it was when the entry was
    /// written.
    pub revision: MigrationRevision,

    /// How far the migration got.
    pub state: EntryState,
}

/// How far the migration of a ledger entry got.
#[derive(Debug)]
pub enum EntryState {
    /// The migration was marked as started and never as finished.
    Started,

    /// The migration finished.
    Finished,
}
