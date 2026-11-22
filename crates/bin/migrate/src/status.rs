//! The `status` and `check` commands: how a database stands against the
//! migrations of this build.

use waymark_migrations_engine::ValidMigrationsArray;
use waymark_migrations_engine::migrations_comparison::{ConflictError, MigrationsComparison};
use waymark_migrations_engine_backend::FindExclusiveMigrationsLock as _;

use crate::Target;
use crate::report::{PendingMigration, Status};

/// Error returned by [`status`].
#[derive(Debug, thiserror::Error)]
pub enum Error<AssertionCheckError> {
    /// The connection could not be made.
    #[error("connecting: {0}")]
    Connect(#[source] waymark_postgres_bringup::ConnectionError),

    /// The status of the migrations could not be told.
    #[error("migrations status: {0}")]
    Status(
        #[source]
        waymark_migrations_engine::migrations_status::Error<AssertionCheckError, sqlx::Error>,
    ),

    /// Whether the exclusive migrations lock is taken could not be told.
    #[error("looking for the exclusive migrations lock: {0}")]
    FindExclusiveLock(#[source] sqlx::Error),

    /// The connection could not be closed.
    #[error("closing the connection: {0}")]
    Close(#[source] sqlx::Error),
}

/// Tell whether the database of `target` meets its assertions, how it
/// stands against its migrations, and whether a migration is in progress
/// on it. No lock is taken, and every
/// transaction on the connection defaults to read-only, so nothing is
/// written. The answer is for the moment of the reading alone.
pub async fn status<Tables, Assertion, Migrations, Migration>(
    target: &Target<'_, Assertion, Migrations>,
) -> Result<Status, Error<waymark_migrations_engine_assertion::CheckErrorFor<Assertion>>>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationIdentity,
{
    let connection =
        waymark_postgres_bringup::connection(waymark_postgres_bringup::ConnectionParams {
            config: target.connection,
            transactions: waymark_postgres_bringup::Transactions::ReadOnly,
            schema: target
                .schema
                .map(|schema| waymark_postgres_bringup::Schema::Require(schema.into())),
        })
        .await;

    let connection = match connection {
        Ok(connection) => connection,
        // Only the migrations create the schema: without it, the
        // database was never migrated.
        Err(waymark_postgres_bringup::ConnectionError::ConnectRequiringSchema(
            waymark_sqlx_postgres_schema_pool::ExistingSchemaError::Missing { .. },
        )) => {
            return Ok(Status::NotMigrated {
                pending: pending(target.migrations.as_slice()),
            });
        }
        Err(error) => return Err(Error::Connect(error)),
    };

    let mut backend =
        waymark_migrations_engine_backend_sqlx_postgres::Backend::<Tables>::new(connection);

    let read = read(&mut backend, target.assertions, target.migrations).await;

    let closed = sqlx::Connection::close(backend.into_connection()).await;

    // A failed reading is the error worth reporting; when the close fails
    // too, its error is dropped: the connection is gone either way.
    let status = read?;

    closed.map_err(Error::Close)?;

    Ok(status)
}

/// The reading itself, through the open connection.
async fn read<Tables, Assertion, Migrations, Migration>(
    backend: &mut waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
    assertions: &Assertion,
    migrations: &ValidMigrationsArray<Migrations>,
) -> Result<Status, Error<waymark_migrations_engine_assertion::CheckErrorFor<Assertion>>>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationIdentity,
{
    let comparison =
        waymark_migrations_engine::migrations_status(backend, assertions, migrations).await;

    let migrations = migrations.as_slice();

    let comparison = match comparison {
        // The engine looked no further than the assertion: neither the
        // ledger nor a lock was looked for.
        Err(waymark_migrations_engine::migrations_status::Error::Assertions(
            waymark_migrations_engine::migrations_engine_assertions::Error::Violated { message },
        )) => {
            return Ok(Status::AssertionViolated { message });
        }
        // No migrations engine tables: no lock to look for either.
        Ok(MigrationsComparison::NotSetUp) => {
            return Ok(Status::NotMigrated {
                pending: pending(migrations),
            });
        }
        Ok(MigrationsComparison::InOrder { applied }) => Ok(applied),
        Err(waymark_migrations_engine::migrations_status::Error::Conflict(conflict)) => {
            Err(conflict)
        }
        Err(error) => return Err(Error::Status(error)),
    };

    let found = backend
        .find_exclusive_migrations_lock()
        .await
        .map_err(Error::FindExclusiveLock)?;

    let migration_in_progress = match found {
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::Found => true,
        waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::NotFound => {
            false
        }
    };

    let status = match comparison {
        Ok(applied) => match migrations.get(applied..) {
            None | Some([]) => Status::UpToDate {
                applied,
                migration_in_progress,
            },
            Some(not_applied) => Status::Pending {
                applied,
                pending: pending(not_applied),
                migration_in_progress,
            },
        },
        Err(ConflictError::Dirty { version }) => Status::Dirty {
            version: version.to_raw(),
            migration_in_progress,
        },
        Err(conflict) => Status::LedgerMismatch {
            detail: conflict.to_string(),
            migration_in_progress,
        },
    };

    Ok(status)
}

/// `migrations` as the report lists the ones that are not applied.
fn pending<Migration>(migrations: &[Migration]) -> Vec<PendingMigration>
where
    Migration: waymark_migrations_engine_migration::HasMigrationIdentity,
{
    migrations
        .iter()
        .map(|migration| {
            let identity = migration.migration_identity();

            PendingMigration {
                version: identity.version.to_raw(),
                description: identity.description.to_owned(),
            }
        })
        .collect()
}
