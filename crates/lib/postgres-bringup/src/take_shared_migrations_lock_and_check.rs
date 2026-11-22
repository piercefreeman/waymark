//! The entry point of a process that runs on a database it does not
//! migrate.

use std::borrow::Cow;

use waymark_migrations_engine::migrations_lock_keeper::TakeSharedError;
use waymark_migrations_engine::{MigrationsLockKeeper, ValidMigrationsArray};
use waymark_postgres_config::migrations_lock;
use waymark_secret_string::SecretString;

use crate::migrations_support::{lock_backend, release_and_close};
use crate::{ConnectionError, HeldSharedMigrationsLock};

/// What [`take_shared_migrations_lock_and_check`] works with.
#[derive(Debug)]
pub struct Params<'a, Assertion, Migrations> {
    /// The URL the shared migrations lock is taken and kept through: the
    /// one the process runs on the database with.
    pub lock_url: &'a SecretString,

    /// The migrations lock's config.
    pub migrations_lock: &'a migrations_lock::Config,

    /// The schema the database's tables are in, which has to be there
    /// already; unscoped when `None`.
    pub schema: Option<Cow<'a, str>>,

    /// The holder the migrations lock is taken for.
    pub holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId,

    /// The assertions the database has to meet.
    pub assertions: &'a Assertion,

    /// The migrations the database has to be migrated to.
    pub migrations: &'a ValidMigrationsArray<Migrations>,
}

/// Error returned by [`take_shared_migrations_lock_and_check`].
#[derive(Debug, thiserror::Error)]
pub enum Error<AssertionCheckError> {
    /// The connection of the shared migrations lock could not be made, or
    /// the schema is not there.
    #[error("connecting for the shared migrations lock: {0}")]
    Connect(#[source] ConnectionError),

    /// Before the lock was taken, the database was not migrated exactly to
    /// the migrations.
    #[error("checking the migrations: {0}")]
    Check(
        #[source]
        waymark_migrations_engine::check_migrations::Error<AssertionCheckError, sqlx::Error>,
    ),

    /// A shared migrations lock was not taken.
    #[error("shared migrations lock: {0}")]
    TakeLock(#[source] TakeSharedError<sqlx::Error>),

    /// Under the shared migrations lock, the database is not migrated
    /// exactly to the migrations.
    #[error("checking the migrations under the shared migrations lock: {0}")]
    CheckUnderLock(
        #[source]
        waymark_migrations_engine::check_migrations::Error<AssertionCheckError, sqlx::Error>,
    ),
}

/// Take a shared migrations lock on a database and check, under it, that
/// the database is migrated exactly to `params.migrations`, for a process
/// that then runs on it. Nothing is migrated and nothing is created.
///
/// The database is checked once before the lock is taken too, so one that
/// was never migrated is told apart from one with a migration in
/// progress.
///
/// On an error no lock is left behind, as far as releasing one succeeds.
pub async fn take_shared_migrations_lock_and_check<Tables, Assertion, Migrations, Migration>(
    params: Params<'_, Assertion, Migrations>,
) -> Result<
    HeldSharedMigrationsLock<Tables>,
    Error<waymark_migrations_engine_assertion::CheckErrorFor<Assertion>>,
>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::HasMigrationMetadata,
{
    let Params {
        lock_url,
        migrations_lock,
        schema,
        holder_id,
        assertions,
        migrations,
    } = params;

    let mut backend = lock_backend::<Tables>(lock_url, migrations_lock, schema.as_deref())
        .await
        .map_err(Error::Connect)?;

    let checked =
        waymark_migrations_engine::check_migrations(&mut backend, assertions, migrations).await;

    if let Err(error) = checked {
        // The error is the one worth reporting; a close that fails too is
        // dropped.
        let _closed = sqlx::Connection::close(backend.into_connection()).await;

        return Err(Error::Check(error));
    }

    let (mut keeper, lock) =
        MigrationsLockKeeper::take_shared(backend, holder_id, migrations_lock.timing.clone())
            .await
            .map_err(Error::TakeLock)?;

    let checked_under_lock =
        waymark_migrations_engine::check_migrations(keeper.backend(), assertions, migrations).await;

    if let Err(error) = checked_under_lock {
        release_and_close(keeper).await;

        return Err(Error::CheckUnderLock(error));
    }

    Ok(HeldSharedMigrationsLock { keeper, lock })
}
