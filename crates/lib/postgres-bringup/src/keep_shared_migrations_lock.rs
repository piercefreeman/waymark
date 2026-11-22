//! Keeping the shared migrations lock of a running process.

use waymark_migrations_engine::MigrationsLockKeeper;
use waymark_migrations_engine::migrations_lock_keeper::KeepOutcome;

/// Error returned by [`keep_shared_migrations_lock`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The time the lock is relied on for passed with no confirmed
    /// renewal. The lock is fenced.
    #[error("the shared migrations lock lapsed: no renewal was confirmed in time")]
    Lapsed,

    /// A renewal found the process without its lock. The lock is fenced.
    #[error("the shared migrations lock is lost: a renewal found it gone")]
    Lost,

    /// The lock could not be released; it stays until it expires.
    #[error("releasing the shared migrations lock: {0}")]
    Release(#[source] sqlx::Error),

    /// The lock's connection could not be closed.
    #[error("closing the connection of the shared migrations lock: {0}")]
    Close(#[source] sqlx::Error),
}

/// Keep the shared migrations lock of `keeper` renewed until `stop`
/// completes, then release it and close its connection.
///
/// A lock that lapses or is lost before that is the error returned, once
/// the lock was released and its connection closed as far as that
/// succeeds: the process must not run on the database any more.
pub async fn keep_shared_migrations_lock<Tables, Stop>(
    mut keeper: MigrationsLockKeeper<
        waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
    >,
    stop: Stop,
) -> Result<(), Error>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Stop: Future<Output = ()>,
{
    let kept = keeper.keep(stop).await;

    let (backend, released) = keeper.release().await;

    let closed = sqlx::Connection::close(backend.into_connection()).await;

    // A fenced lock is the error worth reporting; a release or a close
    // that fails too is dropped.
    match kept {
        KeepOutcome::Stopped => {}
        KeepOutcome::Lapsed => return Err(Error::Lapsed),
        KeepOutcome::Lost => return Err(Error::Lost),
    }

    released.map_err(Error::Release)?;

    closed.map_err(Error::Close)?;

    Ok(())
}
