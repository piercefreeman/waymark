//! What the migrations entry points share.

use std::num::NonZeroUsize;

use waymark_migrations_engine::migrations_lock_keeper::{KeepOutcome, TakeExclusiveError};
use waymark_migrations_engine::run_migrations::Outcome;
use waymark_migrations_engine::{
    ExclusiveMigrationsLock, MigrationsComparison, MigrationsLockKeeper, MigrationsRunTransactions,
    SharedMigrationsLock, ValidMigrationsArray,
};
use waymark_migrations_engine_backend::SetupMigrationsEngine as _;
use waymark_postgres_config::{connection, migrations_lock};
use waymark_secret_string::SecretString;

use crate::{ConnectionError, ConnectionParams, Schema, Transactions};

/// A shared migrations lock a process took, with the keeper that keeps it
/// renewed through a connection of its own.
///
/// [`crate::keep_shared_migrations_lock()`] keeps the lock for the process
/// and releases it. Nothing renews the lock before that: the time it is
/// relied on for counts from when it was taken, and what the entry point
/// that took it did under it afterwards is part of that time.
#[derive_where::derive_where(Debug)]
pub struct HeldSharedMigrationsLock<Tables> {
    /// The keeper of the lock.
    pub keeper:
        MigrationsLockKeeper<waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>>,

    /// The lock.
    pub lock: SharedMigrationsLock,
}

/// Error of migrating a database under the exclusive migrations lock.
#[derive(Debug, thiserror::Error)]
pub enum MigrateUnderExclusiveMigrationsLockError<AssertionCheckError, RunError> {
    /// The status of the migrations could not be told, or the migrations
    /// ledger conflicts with the migrations.
    #[error("migrations status: {0}")]
    Status(
        #[source]
        waymark_migrations_engine::migrations_status::Error<AssertionCheckError, sqlx::Error>,
    ),

    /// The migrations engine could not be set up in the database.
    #[error("setting the migrations engine up: {0}")]
    Setup(#[source] sqlx::Error),

    /// The connection of the exclusive migrations lock could not be made.
    #[error("connecting for the exclusive migrations lock: {0}")]
    ConnectForLock(#[source] ConnectionError),

    /// The exclusive migrations lock was not taken.
    #[error("exclusive migrations lock: {0}")]
    TakeLock(#[source] TakeExclusiveError<sqlx::Error, sqlx::Error>),

    /// The migrations failed.
    #[error("running the migrations: {0}")]
    Run(#[source] RunError),

    /// The exclusive migrations lock was fenced while the migrations ran.
    #[error("the exclusive migrations lock was fenced while the migrations ran")]
    LockFenced,
}

/// What migrating a database under the exclusive migrations lock left.
pub(crate) enum Migrating<Tables> {
    /// No migration was pending, so no lock was taken.
    NothingPending {
        /// The holder id the lock would have been taken for.
        holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId,
    },

    /// The run is done and the exclusive migrations lock still held.
    UnderLock {
        /// The keeper of the exclusive lock.
        keeper:
            MigrationsLockKeeper<waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>>,

        /// The exclusive lock.
        lock: ExclusiveMigrationsLock,

        /// How many migrations the run applied; `None` when another
        /// process applied them all between the look at the ledger and the
        /// lock.
        applied: Option<NonZeroUsize>,
    },
}

/// What a migration under the exclusive migrations lock needs.
pub(crate) struct MigratingParams<'a, Assertion, Migrations> {
    /// The URL the exclusive lock's connection goes to.
    pub lock_url: &'a SecretString,

    /// The migrations lock's config.
    pub migrations_lock: &'a migrations_lock::Config,

    /// The schema the lock's connection is scoped to; it is there already.
    pub schema: Option<&'a str>,

    /// The holder the lock is taken for.
    pub holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId,

    /// The assertions the database has to meet.
    pub assertions: &'a Assertion,

    /// The migrations to migrate the database to.
    pub migrations: &'a ValidMigrationsArray<Migrations>,

    /// What the run does about transactions.
    pub transactions: MigrationsRunTransactions,
}

/// A backend over a connection of its own to `url`, for a migrations
/// lock's statements, scoped to `schema`, which has to be there already.
pub(crate) async fn lock_backend<Tables>(
    url: &SecretString,
    migrations_lock: &migrations_lock::Config,
    schema: Option<&str>,
) -> Result<waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>, ConnectionError> {
    let config = connection::Config {
        url: url.clone(),
        statement_timeout: migrations_lock.statement_timeout,
    };

    let connection = crate::connection(ConnectionParams {
        config: &config,
        transactions: Transactions::Unrestricted,
        schema: schema.map(|schema| Schema::Require(schema.into())),
    })
    .await?;

    Ok(waymark_migrations_engine_backend_sqlx_postgres::Backend::new(connection))
}

/// Release the lock of `keeper` and close its connection. A release or a
/// close that fails is dropped: this is the way out of an error that is
/// the one worth reporting, and the lock expires either way.
pub(crate) async fn release_and_close<Tables>(
    keeper: MigrationsLockKeeper<waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>>,
) where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
{
    let (backend, _released) = keeper.release().await;

    let _closed = sqlx::Connection::close(backend.into_connection()).await;
}

/// Migrate the database through `migrations_backend`, when the ledger it
/// reads has a migration pending: set the migrations engine up, take the
/// exclusive migrations lock on a connection of its own, and run the
/// migrations with the lock kept renewed meanwhile.
///
/// The database's assertions are checked before anything else: when one
/// does not hold, nothing is set up and no lock is taken.
///
/// On an error the lock, when taken, is released.
pub(crate) async fn migrate_under_exclusive_migrations_lock<
    Tables,
    Assertion,
    Migrations,
    Migration,
>(
    migrations_backend: &mut waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
    params: MigratingParams<'_, Assertion, Migrations>,
) -> Result<
    Migrating<Tables>,
    MigrateUnderExclusiveMigrationsLockError<
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
    let MigratingParams {
        lock_url,
        migrations_lock,
        schema,
        holder_id,
        assertions,
        migrations,
        transactions,
    } = params;

    let status =
        waymark_migrations_engine::migrations_status(migrations_backend, assertions, migrations)
            .await
            .map_err(MigrateUnderExclusiveMigrationsLockError::Status)?;

    let pending = match status {
        MigrationsComparison::NotSetUp => true,
        MigrationsComparison::InOrder { applied } => applied < migrations.as_slice().len(),
    };

    if !pending {
        return Ok(Migrating::NothingPending { holder_id });
    }

    migrations_backend
        .setup_migrations_engine()
        .await
        .map_err(MigrateUnderExclusiveMigrationsLockError::Setup)?;

    let lock_backend = lock_backend::<Tables>(lock_url, migrations_lock, schema)
        .await
        .map_err(MigrateUnderExclusiveMigrationsLockError::ConnectForLock)?;

    let (mut keeper, lock) = MigrationsLockKeeper::take_exclusive(
        lock_backend,
        holder_id,
        migrations_lock.timing.clone(),
    )
    .await
    .map_err(MigrateUnderExclusiveMigrationsLockError::TakeLock)?;

    // The run and the keeping go on together in this one task; the keeping
    // stops once the run returned, and neither is dropped before it
    // returns.
    let (run_returned, run_returns) = tokio::sync::oneshot::channel::<()>();

    let (ran, kept) = tokio::join!(
        async {
            let ran = waymark_migrations_engine::run_migrations(
                migrations_backend,
                assertions,
                migrations,
                &lock,
                transactions,
            )
            .await;

            drop(run_returned);

            ran
        },
        keeper.keep(async {
            // The sender is dropped, never sent through: the error of
            // that is the signal.
            let _run_returned = run_returns.await;
        }),
    );

    let outcome = match ran {
        Ok(outcome) => outcome,
        Err(error) => {
            release_and_close(keeper).await;

            return Err(MigrateUnderExclusiveMigrationsLockError::Run(error));
        }
    };

    match kept {
        KeepOutcome::Stopped => {}
        KeepOutcome::Lapsed | KeepOutcome::Lost => {
            release_and_close(keeper).await;

            return Err(MigrateUnderExclusiveMigrationsLockError::LockFenced);
        }
    }

    let applied = match outcome {
        Outcome::NothingPending => None,
        Outcome::Applied { migrations } => Some(migrations),
    };

    Ok(Migrating::UnderLock {
        keeper,
        lock,
        applied,
    })
}
