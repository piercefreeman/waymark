//! Migrations ledger reading trait.

use crate::MigrationsLedger;

/// Backend capability for reading the migrations ledger.
pub trait ReadMigrationsLedger {
    /// An error that can occur while reading.
    type Error;

    /// Read the migrations ledger. A database the migrations engine is not
    /// set up in is not an error: it reads as
    /// [`MigrationsLedger::NotSetUp`].
    fn read_migrations_ledger(
        &mut self,
    ) -> impl Future<Output = Result<MigrationsLedger, Self::Error>> + Send + '_;
}
