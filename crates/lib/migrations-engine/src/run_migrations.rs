//! The run flow: applies the migrations of a migrations array that a
//! database does not have yet.

use std::num::NonZeroUsize;

use waymark_migrations_engine_migration::{MigrationTransactionSupport, MigrationVersion};

use crate::ExclusiveMigrationsLock;
use crate::migrations_comparison::{
    ConflictError, MigrationsComparison, ValidMigrationsArray, compare_migrations,
};
use crate::migrations_engine_assertions::check_migrations_engine_assertions;

/// What a migrations run does about transactions.
///
/// Either way each migration is marked as started before it runs and as
/// finished after.
#[derive(Debug)]
pub enum MigrationsRunTransactions {
    /// One transaction goes around the whole run: every pending migration
    /// and its ledger entry are committed together, or none is. A run with
    /// a pending migration that cannot run inside a transaction is refused
    /// before anything is written.
    WholeRun,

    /// The run begins no transaction: a migration that fails stays marked
    /// as started.
    Unmanaged,
}

/// What a migrations run did.
#[derive(Debug)]
pub enum Outcome {
    /// Every migration of the array was applied already.
    NothingPending,

    /// The run applied the pending migrations.
    Applied {
        /// How many migrations the run applied.
        migrations: NonZeroUsize,
    },
}

/// Error returned by [`run_migrations`].
#[derive(Debug, thiserror::Error)]
pub enum Error<
    AssertionError,
    ReadLedgerError,
    BeginError,
    CommitError,
    MarkStartedError,
    MarkFinishedError,
    PreValidateError,
    RunError,
    PostValidateError,
> {
    /// The database does not meet an assertion, or one could not be
    /// checked.
    #[error("migrations engine assertions: {0}")]
    Assertions(#[source] crate::migrations_engine_assertions::Error<AssertionError>),

    /// The backend failed to read the migrations ledger.
    #[error("reading the migrations ledger: {0}")]
    ReadLedger(#[source] ReadLedgerError),

    /// The migrations engine is not set up in the database.
    #[error("the migrations engine is not set up in the database")]
    NotSetUp,

    /// The migrations ledger conflicts with the migrations array.
    #[error("migrations ledger: {0}")]
    Conflict(#[source] ConflictError),

    /// A pending migration cannot run inside a transaction, and the run
    /// goes in one.
    #[error(
        "migration {} cannot run inside a transaction, and the whole run goes in one",
        .version.to_raw()
    )]
    TransactionUnsupported {
        /// The version of the migration.
        version: MigrationVersion,
    },

    /// The exclusive migrations lock was fenced before the run was done.
    #[error("the exclusive migrations lock is fenced")]
    LockFenced,

    /// The backend failed to begin the whole-run transaction.
    #[error("beginning the whole-run transaction: {0}")]
    Begin(#[source] BeginError),

    /// The backend failed to commit the whole-run transaction.
    #[error("committing the whole-run transaction: {0}")]
    Commit(#[source] CommitError),

    /// The backend failed to mark a migration as started.
    #[error("marking migration {} as started: {error}", .version.to_raw())]
    MarkStarted {
        /// The version of the migration.
        version: MigrationVersion,

        /// The backend's error.
        #[source]
        error: MarkStartedError,
    },

    /// The backend failed to mark a migration as finished.
    #[error("marking migration {} as finished: {error}", .version.to_raw())]
    MarkFinished {
        /// The version of the migration.
        version: MigrationVersion,

        /// The backend's error.
        #[source]
        error: MarkFinishedError,
    },

    /// A migration's pre-validation failed.
    #[error("pre-validating migration {}: {error}", .version.to_raw())]
    PreValidate {
        /// The version of the migration.
        version: MigrationVersion,

        /// The migration's error.
        #[source]
        error: PreValidateError,
    },

    /// A migration failed to run.
    #[error("running migration {}: {error}", .version.to_raw())]
    Run {
        /// The version of the migration.
        version: MigrationVersion,

        /// The migration's error.
        #[source]
        error: RunError,
    },

    /// A migration's post-validation failed.
    #[error("post-validating migration {}: {error}", .version.to_raw())]
    PostValidate {
        /// The version of the migration.
        version: MigrationVersion,

        /// The migration's error.
        #[source]
        error: PostValidateError,
    },
}

/// Shorthand for the [`Error`] of a run through the backend `Backend` with
/// assertions of the type `Assertion` and migrations of the type
/// `Migration`.
pub type ErrorFor<Backend, Assertion, Migration> = Error<
    waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
    <Backend as waymark_migrations_engine_backend::ReadMigrationsLedger>::Error,
    <Backend as waymark_migrations_engine_backend::BeginWholeRunTransaction>::Error,
    <Backend as waymark_migrations_engine_backend::CommitWholeRunTransaction>::Error,
    <Backend as waymark_migrations_engine_backend::MarkMigrationStarted>::Error,
    <Backend as waymark_migrations_engine_backend::MarkMigrationFinished>::Error,
    <Migration as waymark_migrations_engine_migration::PreValidateMigration>::Error,
    <Migration as waymark_migrations_engine_migration::RunMigration>::Error,
    <Migration as waymark_migrations_engine_migration::PostValidateMigration>::Error,
>;

/// Apply, through `backend`, the ones of `migrations` the database does
/// not have yet, in the order of the array: each is pre-validated, run and
/// post-validated on the connection `backend` holds.
///
/// `assertions` are checked first, on that connection: when one does not
/// hold, the ledger is not read and nothing is applied.
///
/// `lock` is the exclusive migrations lock the run goes under. It is
/// looked at before each migration, and before the commit of a whole-run
/// transaction: once it is fenced the run stops there, without starting
/// another migration. A migration on its way is never dropped.
///
/// Under [`MigrationsRunTransactions::WholeRun`] an error after the
/// transaction began rolls it back; a rollback that fails is logged, and
/// the error that caused it is the one returned.
pub async fn run_migrations<Backend, Assertion, Migrations, Migration>(
    backend: &mut Backend,
    assertions: &Assertion,
    migrations: &ValidMigrationsArray<Migrations>,
    lock: &ExclusiveMigrationsLock,
    transactions: MigrationsRunTransactions,
) -> Result<Outcome, ErrorFor<Backend, Assertion, Migration>>
where
    Assertion: waymark_migrations_engine_assertion::Assertion<
            Connection = <Backend as waymark_migrations_engine_backend::HasConnection>::Connection,
        >,
    Migrations: AsRef<[Migration]>,
    Backend: waymark_migrations_engine_backend::AccessMigrationsConnection,
    Backend: waymark_migrations_engine_backend::ReadMigrationsLedger,
    Backend: waymark_migrations_engine_backend::BeginWholeRunTransaction,
    Backend: waymark_migrations_engine_backend::CommitWholeRunTransaction,
    Backend: waymark_migrations_engine_backend::RollbackWholeRunTransaction,
    Backend: waymark_migrations_engine_backend::MarkMigrationStarted,
    Backend: waymark_migrations_engine_backend::MarkMigrationFinished,
    <Backend as waymark_migrations_engine_backend::RollbackWholeRunTransaction>::Error:
        std::fmt::Display,
    Migration: waymark_migrations_engine_migration::Migration<
            Connection = <Backend as waymark_migrations_engine_backend::HasConnection>::Connection,
        >,
{
    check_migrations_engine_assertions(backend.migrations_connection(), assertions)
        .await
        .map_err(Error::Assertions)?;

    let ledger = backend
        .read_migrations_ledger()
        .await
        .map_err(Error::ReadLedger)?;

    let comparison = compare_migrations(&ledger, migrations).map_err(Error::Conflict)?;

    let migrations = migrations.as_slice();

    let applied = match comparison {
        MigrationsComparison::NotSetUp => return Err(Error::NotSetUp),
        MigrationsComparison::InOrder { applied } => applied,
    };

    // The comparison counted `applied` out of `migrations`, so the rest is
    // there; an empty rest is taken the same way as a missing one.
    let pending = migrations.get(applied..).unwrap_or_default();

    let Some(pending_count) = NonZeroUsize::new(pending.len()) else {
        return Ok(Outcome::NothingPending);
    };

    match transactions {
        MigrationsRunTransactions::WholeRun => {
            for migration in pending {
                match migration.migration_transaction_support() {
                    MigrationTransactionSupport::Supported => {}
                    MigrationTransactionSupport::Unsupported => {
                        return Err(Error::TransactionUnsupported {
                            version: migration.migration_metadata().version,
                        });
                    }
                }
            }

            backend
                .begin_whole_run_transaction()
                .await
                .map_err(Error::Begin)?;

            let ran = apply_pending::<_, Assertion, _>(backend, pending, lock).await;

            // The lock is looked at once more after the last migration,
            // for the commit that follows.
            let ran = match ran {
                Ok(()) if lock.is_migrations_lock_fenced() => Err(Error::LockFenced),
                Ok(()) => Ok(()),
                Err(error) => Err(error),
            };

            if let Err(error) = ran {
                let rolled_back = backend.rollback_whole_run_transaction().await;

                if let Err(rollback_error) = rolled_back {
                    tracing::warn!(
                        error = %rollback_error,
                        "rolling back the whole-run transaction failed"
                    );
                }

                return Err(error);
            }

            backend
                .commit_whole_run_transaction()
                .await
                .map_err(Error::Commit)?;
        }
        MigrationsRunTransactions::Unmanaged => {
            apply_pending::<_, Assertion, _>(backend, pending, lock).await?;
        }
    }

    Ok(Outcome::Applied {
        migrations: pending_count,
    })
}

/// Apply `pending`, in order: each migration is marked as started once
/// its pre-validation passed, and as finished once its post-validation
/// passed. Whether a transaction is around all of it is the caller's.
async fn apply_pending<Backend, Assertion, Migration>(
    backend: &mut Backend,
    pending: &[Migration],
    lock: &ExclusiveMigrationsLock,
) -> Result<(), ErrorFor<Backend, Assertion, Migration>>
where
    Assertion: waymark_migrations_engine_assertion::Assertion,
    Backend: waymark_migrations_engine_backend::AccessMigrationsConnection,
    Backend: waymark_migrations_engine_backend::ReadMigrationsLedger,
    Backend: waymark_migrations_engine_backend::BeginWholeRunTransaction,
    Backend: waymark_migrations_engine_backend::CommitWholeRunTransaction,
    Backend: waymark_migrations_engine_backend::MarkMigrationStarted,
    Backend: waymark_migrations_engine_backend::MarkMigrationFinished,
    Migration: waymark_migrations_engine_migration::Migration<
            Connection = <Backend as waymark_migrations_engine_backend::HasConnection>::Connection,
        >,
{
    for migration in pending {
        let metadata = migration.migration_metadata();
        let version = metadata.version;

        if lock.is_migrations_lock_fenced() {
            return Err(Error::LockFenced);
        }

        migration
            .pre_validate_migration(backend.migrations_connection())
            .await
            .map_err(|error| Error::PreValidate { version, error })?;

        backend
            .mark_migration_started(metadata)
            .await
            .map_err(|error| Error::MarkStarted { version, error })?;

        migration
            .run_migration(backend.migrations_connection())
            .await
            .map_err(|error| Error::Run { version, error })?;

        migration
            .post_validate_migration(backend.migrations_connection())
            .await
            .map_err(|error| Error::PostValidate { version, error })?;

        backend
            .mark_migration_finished(version)
            .await
            .map_err(|error| Error::MarkFinished { version, error })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;
