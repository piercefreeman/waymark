//! The entry point of a process that migrates a database and does not
//! run on it.

use std::borrow::Cow;

use waymark_migrations_engine::run_migrations::Outcome;
use waymark_migrations_engine::{MigrationsRunTransactions, ValidMigrationsArray};
use waymark_postgres_config::{connection, migrations_lock};

use crate::migrations_support::{
    Migrating, MigratingParams, migrate_under_exclusive_migrations_lock,
};
use crate::{
    ConnectionError, ConnectionParams, MigrateUnderExclusiveMigrationsLockError, Schema,
    Transactions,
};

/// What [`migrate_and_release_migrations_lock`] works with.
#[derive(Debug)]
pub struct Params<'a, Assertion, Migrations> {
    /// The config of the connection the migrations run through. The
    /// exclusive migrations lock is taken and kept through another
    /// connection to the same URL.
    pub migrations_connection: &'a connection::Config,

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

/// Error returned by [`migrate_and_release_migrations_lock`].
#[derive(Debug, thiserror::Error)]
pub enum Error<AssertionCheckError, RunError> {
    /// The migrations connection could not be made.
    #[error("connecting for the migrations: {0}")]
    ConnectForMigrations(#[source] ConnectionError),

    /// The database was not migrated.
    #[error("migrating: {0}")]
    Migrate(#[source] MigrateUnderExclusiveMigrationsLockError<AssertionCheckError, RunError>),

    /// The exclusive migrations lock could not be released; it stays
    /// until it expires.
    #[error("releasing the exclusive migrations lock: {0}")]
    ReleaseLock(#[source] sqlx::Error),

    /// The connection of the exclusive migrations lock could not be
    /// closed.
    #[error("closing the connection of the exclusive migrations lock: {0}")]
    CloseLockConnection(#[source] sqlx::Error),

    /// The migrations connection could not be closed.
    #[error("closing the migrations connection: {0}")]
    CloseMigrationsConnection(#[source] sqlx::Error),
}

/// Migrate a database to `params.migrations`, for a process that does not
/// run on it afterwards.
///
/// With a migration pending, the exclusive migrations lock is taken, the
/// migrations run under it, and it is released. With none pending, no lock
/// is taken. The connections are closed either way.
pub async fn migrate_and_release_migrations_lock<Tables, Assertion, Migrations, Migration>(
    params: Params<'_, Assertion, Migrations>,
) -> Result<
    Outcome,
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
        migrations_connection: migrations_connection_config,
        migrations_lock,
        schema,
        holder_id,
        assertions,
        migrations,
        transactions,
    } = params;

    let schema = schema.as_deref();

    let migrations_connection = crate::connection(ConnectionParams {
        config: migrations_connection_config,
        transactions: Transactions::Unrestricted,
        schema: schema.map(|schema| Schema::Create(schema.into())),
    })
    .await
    .map_err(Error::ConnectForMigrations)?;

    let mut migrations_backend =
        waymark_migrations_engine_backend_sqlx_postgres::Backend::<Tables>::new(
            migrations_connection,
        );

    let migrating = migrate_under_exclusive_migrations_lock(
        &mut migrations_backend,
        MigratingParams {
            lock_url: &migrations_connection_config.url,
            migrations_lock,
            schema,
            holder_id,
            assertions,
            migrations,
            transactions,
        },
    )
    .await;

    let closed = sqlx::Connection::close(migrations_backend.into_connection()).await;

    // An error before the close is the one worth reporting; when the close
    // fails too, its error is dropped: the connection is gone either way.
    let migrating = migrating.map_err(Error::Migrate)?;

    let outcome = match migrating {
        Migrating::NothingPending { holder_id: _ } => Outcome::NothingPending,
        Migrating::UnderLock {
            keeper,
            lock: _,
            applied,
        } => {
            let (lock_backend, released) = keeper.release().await;

            let lock_connection_closed =
                sqlx::Connection::close(lock_backend.into_connection()).await;

            released.map_err(Error::ReleaseLock)?;

            lock_connection_closed.map_err(Error::CloseLockConnection)?;

            match applied {
                None => Outcome::NothingPending,
                Some(migrations) => Outcome::Applied { migrations },
            }
        }
    };

    closed.map_err(Error::CloseMigrationsConnection)?;

    Ok(outcome)
}
