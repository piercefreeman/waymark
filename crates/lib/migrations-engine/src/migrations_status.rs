//! The status flow: how a database's migrations ledger stands against a
//! migrations array.

use crate::migrations_comparison::{
    ConflictError, MigrationsComparison, ValidMigrationsArray, compare_migrations,
};
use crate::migrations_engine_assertions::check_migrations_engine_assertions;

/// Error returned by [`migrations_status`].
#[derive(Debug, thiserror::Error)]
pub enum Error<AssertionError, ReadLedgerError> {
    /// The database does not meet an assertion, or one could not be
    /// checked.
    #[error("migrations engine assertions: {0}")]
    Assertions(#[source] crate::migrations_engine_assertions::Error<AssertionError>),

    /// The backend failed to read the migrations ledger.
    #[error("reading the migrations ledger: {0}")]
    ReadLedger(#[source] ReadLedgerError),

    /// The migrations ledger conflicts with the migrations array.
    #[error("migrations ledger: {0}")]
    Conflict(#[source] ConflictError),
}

/// Shorthand for the [`Error`] of a status through the backend `Backend`
/// with assertions of the type `Assertion`.
pub type ErrorFor<Backend, Assertion> = Error<
    waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
    <Backend as waymark_migrations_engine_backend::ReadMigrationsLedger>::Error,
>;

/// Check `assertions` through `backend`, then read the migrations ledger
/// through it and compare it with `migrations`. Nothing is written, and
/// the ledger is not read when an assertion does not hold.
pub async fn migrations_status<Backend, Assertion, Migrations, Migration>(
    backend: &mut Backend,
    assertions: &Assertion,
    migrations: &ValidMigrationsArray<Migrations>,
) -> Result<MigrationsComparison, ErrorFor<Backend, Assertion>>
where
    Backend: waymark_migrations_engine_backend::AccessMigrationsConnection,
    Backend: waymark_migrations_engine_backend::ReadMigrationsLedger,
    Assertion: waymark_migrations_engine_assertion::Assertion<
            Connection = <Backend as waymark_migrations_engine_backend::HasConnection>::Connection,
        >,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationMetadata,
{
    check_migrations_engine_assertions(backend.migrations_connection(), assertions)
        .await
        .map_err(Error::Assertions)?;

    let ledger = backend
        .read_migrations_ledger()
        .await
        .map_err(Error::ReadLedger)?;

    compare_migrations(&ledger, migrations).map_err(Error::Conflict)
}

#[cfg(test)]
mod tests;
