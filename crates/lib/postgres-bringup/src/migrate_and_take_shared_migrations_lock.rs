//! The entry point of a process that migrates a database and then runs
//! on it.

use std::borrow::Cow;

use waymark_migrations_engine::migrations_lock_keeper::{ConvertToSharedError, TakeSharedError};
use waymark_migrations_engine::{
    MigrationsLockKeeper, MigrationsRunTransactions, ValidMigrationsArray,
};
use waymark_postgres_config::{connection, migrations_lock};
use waymark_secret_string::SecretString;

use crate::migrations_support::{
    Migrating, MigratingParams, lock_backend, migrate_under_exclusive_migrations_lock,
    release_and_close,
};
use crate::{
    ConnectionError, ConnectionParams, HeldSharedMigrationsLock,
    MigrateUnderExclusiveMigrationsLockError, Schema, Transactions,
};

/// What [`migrate_and_take_shared_migrations_lock`] works with.
#[derive(Debug)]
pub struct Params<'a, Assertion, Migrations> {
    /// The config of the connection the migrations run through. The
    /// exclusive migrations lock is taken and kept through another
    /// connection to the same URL.
    pub migrations_connection: &'a connection::Config,

    /// The URL the shared migrations lock is kept through: the one the
    /// process runs on the database with.
    pub shared_lock_url: &'a SecretString,

    /// The migrations lock's config.
    pub migrations_lock: &'a migrations_lock::Config,

    /// The schema the database's tables are in, created if missing;
    /// unscoped when `None`.
    pub schema: Option<Cow<'a, str>>,

    /// The holder the migrations lock is taken for.
    pub holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId,

    /// The assertions the database has to meet.
    pub assertions: &'a Assertion,

    /// The migrations to migrate the database to.
    pub migrations: &'a ValidMigrationsArray<Migrations>,

    /// What the run does about transactions.
    pub transactions: MigrationsRunTransactions,
}

/// Error returned by [`migrate_and_take_shared_migrations_lock`].
#[derive(Debug, thiserror::Error)]
pub enum Error<AssertionCheckError, RunError> {
    /// The migrations connection could not be made.
    #[error("connecting for the migrations: {0}")]
    ConnectForMigrations(#[source] ConnectionError),

    /// The database was not migrated.
    #[error("migrating: {0}")]
    Migrate(#[source] MigrateUnderExclusiveMigrationsLockError<AssertionCheckError, RunError>),

    /// The connection of the shared migrations lock could not be made.
    #[error("connecting for the shared migrations lock: {0}")]
    ConnectForSharedLock(#[source] ConnectionError),

    /// The exclusive migrations lock was not converted to a shared one.
    #[error("exclusive migrations lock: {0}")]
    ConvertLock(#[source] ConvertToSharedError<sqlx::Error>),

    /// A shared migrations lock was not taken.
    #[error("shared migrations lock: {0}")]
    TakeSharedLock(#[source] TakeSharedError<sqlx::Error>),

    /// Under the shared migrations lock, the database is not migrated
    /// exactly to the migrations.
    #[error("checking the migrations: {0}")]
    Check(
        #[source]
        waymark_migrations_engine::check_migrations::Error<AssertionCheckError, sqlx::Error>,
    ),

    /// The connection of the exclusive migrations lock could not be
    /// closed.
    #[error("closing the connection of the exclusive migrations lock: {0}")]
    CloseExclusiveLockConnection(#[source] sqlx::Error),

    /// The migrations connection could not be closed.
    #[error("closing the migrations connection: {0}")]
    CloseMigrationsConnection(#[source] sqlx::Error),
}

/// Migrate a database to `params.migrations` and take a shared migrations
/// lock on it, for a process that then runs on it.
///
/// With a migration pending, the exclusive migrations lock is taken, the
/// migrations run under it, and it is converted to the shared one: the
/// process is at no moment without a lock in between. With none pending, a
/// shared lock is taken at once. Either way the database is then checked
/// against the migrations under the shared lock, and the migrations
/// connection is closed.
///
/// On an error no lock is left behind, as far as releasing one succeeds.
pub async fn migrate_and_take_shared_migrations_lock<Tables, Assertion, Migrations, Migration>(
    params: Params<'_, Assertion, Migrations>,
) -> Result<
    HeldSharedMigrationsLock<Tables>,
    Error<
        waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
        waymark_migrations_engine::run_migrations::ErrorFor<
            waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
            Assertion,
            Migration,
        >,
    >,
>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::Migration<Connection = sqlx::PgConnection>,
{
    let migrations_connection = crate::connection(ConnectionParams {
        config: params.migrations_connection,
        transactions: Transactions::Unrestricted,
        schema: params
            .schema
            .as_deref()
            .map(|schema| Schema::Create(schema.into())),
    })
    .await
    .map_err(Error::ConnectForMigrations)?;

    let mut migrations_backend =
        waymark_migrations_engine_backend_sqlx_postgres::Backend::<Tables>::new(
            migrations_connection,
        );

    let held = through_migrations_backend(&mut migrations_backend, params).await;

    let closed = sqlx::Connection::close(migrations_backend.into_connection()).await;

    // An error before the close is the one worth reporting; when the close
    // fails too, its error is dropped: the connection is gone either way.
    let held = held?;

    if let Err(error) = closed {
        release_and_close(held.keeper).await;

        return Err(Error::CloseMigrationsConnection(error));
    }

    Ok(held)
}

/// What [`migrate_and_take_shared_migrations_lock`] does while the
/// migrations connection is open.
async fn through_migrations_backend<Tables, Assertion, Migrations, Migration>(
    migrations_backend: &mut waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
    params: Params<'_, Assertion, Migrations>,
) -> Result<
    HeldSharedMigrationsLock<Tables>,
    Error<
        waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
        waymark_migrations_engine::run_migrations::ErrorFor<
            waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
            Assertion,
            Migration,
        >,
    >,
>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::Migration<Connection = sqlx::PgConnection>,
{
    let Params {
        migrations_connection,
        shared_lock_url,
        migrations_lock,
        schema,
        holder_id,
        assertions,
        migrations,
        transactions,
    } = params;

    let schema = schema.as_deref();

    let migrating = migrate_under_exclusive_migrations_lock(
        migrations_backend,
        MigratingParams {
            lock_url: &migrations_connection.url,
            migrations_lock,
            schema,
            holder_id,
            assertions,
            migrations,
            transactions,
        },
    )
    .await
    .map_err(Error::Migrate)?;

    let held = match migrating {
        Migrating::NothingPending { holder_id } => {
            let shared_backend = lock_backend::<Tables>(shared_lock_url, migrations_lock, schema)
                .await
                .map_err(Error::ConnectForSharedLock)?;

            let (keeper, lock) = MigrationsLockKeeper::take_shared(
                shared_backend,
                holder_id,
                migrations_lock.timing.clone(),
            )
            .await
            .map_err(Error::TakeSharedLock)?;

            HeldSharedMigrationsLock { keeper, lock }
        }
        Migrating::UnderLock {
            keeper,
            lock,
            applied: _,
        } => {
            let shared_backend =
                match lock_backend::<Tables>(shared_lock_url, migrations_lock, schema).await {
                    Ok(shared_backend) => shared_backend,
                    Err(error) => {
                        release_and_close(keeper).await;

                        return Err(Error::ConnectForSharedLock(error));
                    }
                };

            let converted = keeper
                .convert_to_shared(lock, shared_backend)
                .await
                .map_err(Error::ConvertLock)?;

            let closed =
                sqlx::Connection::close(converted.exclusive_backend.into_connection()).await;

            if let Err(error) = closed {
                release_and_close(converted.keeper).await;

                return Err(Error::CloseExclusiveLockConnection(error));
            }

            HeldSharedMigrationsLock {
                keeper: converted.keeper,
                lock: converted.lock,
            }
        }
    };

    let checked =
        waymark_migrations_engine::check_migrations(migrations_backend, assertions, migrations)
            .await;

    if let Err(error) = checked {
        release_and_close(held.keeper).await;

        return Err(Error::Check(error));
    }

    Ok(held)
}
