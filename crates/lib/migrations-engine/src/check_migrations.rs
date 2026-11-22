//! The check flow: whether a database is migrated exactly to a migrations
//! array.

use waymark_migrations_engine_migration::MigrationVersion;

use crate::migrations_comparison::{MigrationsComparison, ValidMigrationsArray};
use crate::migrations_status::migrations_status;

/// Error returned by [`check_migrations`].
#[derive(Debug, thiserror::Error)]
pub enum Error<AssertionError, ReadLedgerError> {
    /// The status of the migrations could not be told: the database does
    /// not meet an assertion, or the migrations ledger conflicts with the
    /// migrations array.
    #[error("migrations status: {0}")]
    Status(#[source] crate::migrations_status::Error<AssertionError, ReadLedgerError>),

    /// The migrations engine is not set up in the database: it was never
    /// migrated.
    #[error("the database has not been migrated")]
    NotMigrated,

    /// A migration of the array is not applied: the database is older than
    /// the array.
    #[error("migration {} is not applied yet", .version.to_raw())]
    Pending {
        /// The version of the first migration that is not applied.
        version: MigrationVersion,
    },
}

/// Shorthand for the [`Error`] of a check through the backend `Backend`
/// with assertions of the type `Assertion`.
pub type ErrorFor<Backend, Assertion> = Error<
    waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
    <Backend as waymark_migrations_engine_backend::ReadMigrationsLedger>::Error,
>;

/// Check, through `backend`, that the database meets `assertions`, that
/// every one of `migrations` is applied and that the migrations ledger has
/// nothing else. Nothing is written.
pub async fn check_migrations<Backend, Assertion, Migrations, Migration>(
    backend: &mut Backend,
    assertions: &Assertion,
    migrations: &ValidMigrationsArray<Migrations>,
) -> Result<(), ErrorFor<Backend, Assertion>>
where
    Backend: waymark_migrations_engine_backend::AccessMigrationsConnection,
    Backend: waymark_migrations_engine_backend::ReadMigrationsLedger,
    Assertion: waymark_migrations_engine_assertion::Assertion<
            Connection = <Backend as waymark_migrations_engine_backend::HasConnection>::Connection,
        >,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationMetadata,
{
    let comparison = migrations_status(backend, assertions, migrations)
        .await
        .map_err(Error::Status)?;

    let applied = match comparison {
        MigrationsComparison::NotSetUp => return Err(Error::NotMigrated),
        MigrationsComparison::InOrder { applied } => applied,
    };

    if let Some(pending) = migrations.as_slice().get(applied) {
        return Err(Error::Pending {
            version: pending.migration_metadata().version,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests;
