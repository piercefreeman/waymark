//! The `run` command: apply the pending migrations.

use waymark_migrations_engine::MigrationsRunTransactions;
use waymark_migrations_engine::migrations_comparison::ConflictError;
use waymark_migrations_engine::migrations_lock_keeper::TakeExclusiveError;
use waymark_migrations_engine::run_migrations::Outcome;
use waymark_postgres_bringup::MigrateUnderExclusiveMigrationsLockError;

use crate::Target;
use crate::cli::Transactions;
use crate::report::{LockRefusedReason, Run};

/// Error returned by [`run`] over the database whose migrations engine
/// tables are `Tables`, with assertions of the type `Assertion`, migrated
/// to an array of `Migration`.
pub type Error<Tables, Assertion, Migration> =
    waymark_postgres_bringup::migrate_and_release_migrations_lock::Error<
        waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
        waymark_migrations_engine::run_migrations::ErrorFor<
            waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
            Assertion,
            Migration,
        >,
    >;

/// Migrate the database of `target` to its migrations under the exclusive
/// migrations lock, taken for a holder of this invocation alone and
/// released afterwards.
///
/// An assertion the database does not meet, a refused lock and a
/// migrations ledger that conflicts with the migrations are what the run
/// did, not errors: nothing was applied.
pub async fn run<Tables, Assertion, Migrations, Migration>(
    target: &Target<'_, Assertion, Migrations>,
    transactions: &Transactions,
) -> Result<Run, Error<Tables, Assertion, Migration>>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::Migration<Connection = sqlx::PgConnection>,
{
    let transactions = match transactions {
        Transactions::WholeRun => MigrationsRunTransactions::WholeRun,
        Transactions::Unmanaged => MigrationsRunTransactions::Unmanaged,
    };

    let migrated =
        waymark_postgres_bringup::migrate_and_release_migrations_lock::<Tables, _, _, _>(
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Params {
                migrations_connection: target.connection,
                migrations_lock: target.migrations_lock,
                schema: target.schema.map(Into::into),
                holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId::from_raw(
                    uuid::Uuid::new_v4(),
                ),
                assertions: target.assertions,
                migrations: target.migrations,
                transactions,
            },
        )
        .await;

    let error = match migrated {
        Ok(Outcome::NothingPending) => return Ok(Run::NothingPending),
        Ok(Outcome::Applied { migrations }) => {
            return Ok(Run::Applied {
                migrations: migrations.get(),
            });
        }
        Err(error) => error,
    };

    use waymark_postgres_bringup::migrate_and_release_migrations_lock::Error::Migrate;

    let conflict = match error {
        // The assertions are checked before the ledger is read, and again
        // under the lock.
        Migrate(MigrateUnderExclusiveMigrationsLockError::Status(
            waymark_migrations_engine::migrations_status::Error::Assertions(
                waymark_migrations_engine::migrations_engine_assertions::Error::Violated {
                    message,
                },
            ),
        ))
        | Migrate(MigrateUnderExclusiveMigrationsLockError::Run(
            waymark_migrations_engine::run_migrations::Error::Assertions(
                waymark_migrations_engine::migrations_engine_assertions::Error::Violated {
                    message,
                },
            ),
        )) => {
            return Ok(Run::AssertionViolated { message });
        }
        Migrate(MigrateUnderExclusiveMigrationsLockError::TakeLock(
            TakeExclusiveError::MigrationInProgress,
        )) => {
            return Ok(Run::LockRefused {
                reason: LockRefusedReason::MigrationInProgress,
            });
        }
        Migrate(MigrateUnderExclusiveMigrationsLockError::TakeLock(
            TakeExclusiveError::DatabaseInUse,
        )) => {
            return Ok(Run::LockRefused {
                reason: LockRefusedReason::DatabaseInUse,
            });
        }
        // The ledger is compared before the lock is taken, and again
        // under it.
        Migrate(MigrateUnderExclusiveMigrationsLockError::Status(
            waymark_migrations_engine::migrations_status::Error::Conflict(conflict),
        ))
        | Migrate(MigrateUnderExclusiveMigrationsLockError::Run(
            waymark_migrations_engine::run_migrations::Error::Conflict(conflict),
        )) => conflict,
        error => return Err(error),
    };

    let run = match conflict {
        ConflictError::Dirty { version } => Run::Dirty {
            version: version.to_raw(),
        },
        conflict => Run::LedgerMismatch {
            detail: conflict.to_string(),
        },
    };

    Ok(run)
}
