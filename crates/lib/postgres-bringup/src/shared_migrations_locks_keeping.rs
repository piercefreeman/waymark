//! Keeping the shared migrations locks of a process for as long as it
//! runs.

use std::pin::Pin;

use futures_util::StreamExt as _;
use waymark_migrations_engine::MigrationsLockKeeper;

/// A shared migrations lock being kept until the keeping is stopped.
type Keeping =
    Pin<Box<dyn Future<Output = Result<(), crate::keep_shared_migrations_lock::Error>> + Send>>;

/// Where a process hands over the keepers of the shared migrations locks
/// it took, one database's after another's, while it starts up.
///
/// The locks are kept by the [`SharedMigrationsLocksKeeping`] this came
/// with. Once every handover is dropped, no more locks are expected.
#[derive(Debug)]
pub struct SharedMigrationsLocksHandover {
    /// Where the locks to keep go.
    sender: tokio::sync::mpsc::UnboundedSender<Keeping>,

    /// Cancelled when the keeping is stopped.
    stop: tokio_util::sync::CancellationToken,
}

/// The keeping of the shared migrations locks a process hands over
/// through the [`SharedMigrationsLocksHandover`] this came with.
#[derive(Debug)]
pub struct SharedMigrationsLocksKeeping {
    /// Where the locks to keep come from.
    receiver: tokio::sync::mpsc::UnboundedReceiver<Keeping>,

    /// Cancelled when the keeping is stopped.
    stop: tokio_util::sync::CancellationToken,
}

/// Error returned by [`SharedMigrationsLocksHandover::keep`]: the keeping
/// has returned or was dropped, so the lock handed over is not kept.
#[derive(Debug, thiserror::Error)]
#[error("the keeping of the shared migrations locks is gone")]
pub struct KeepingGoneError;

/// A handover and the keeping of what goes through it.
pub fn shared_migrations_locks_keeping()
-> (SharedMigrationsLocksHandover, SharedMigrationsLocksKeeping) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let stop = tokio_util::sync::CancellationToken::new();

    let handover = SharedMigrationsLocksHandover {
        sender,
        stop: stop.clone(),
    };
    let keeping = SharedMigrationsLocksKeeping { receiver, stop };

    (handover, keeping)
}

impl SharedMigrationsLocksHandover {
    /// Hand the shared migrations lock of `keeper` over, to be kept
    /// renewed until the keeping is stopped and released then.
    pub fn keep<Tables>(
        &self,
        keeper: MigrationsLockKeeper<
            waymark_migrations_engine_backend_sqlx_postgres::Backend<Tables>,
        >,
    ) -> Result<(), KeepingGoneError>
    where
        Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
        Tables: 'static,
    {
        let keeping: Keeping = Box::pin(crate::keep_shared_migrations_lock(
            keeper,
            self.stop.clone().cancelled_owned(),
        ));

        self.sender.send(keeping).map_err(|_| KeepingGoneError)
    }
}

impl SharedMigrationsLocksKeeping {
    /// Keep every lock handed over renewed until `stop` completes, then
    /// release them all; return once every handover is dropped and every
    /// lock handed over is released.
    ///
    /// `stop` is meant to complete only once nothing of the process runs
    /// on the databases any more: the locks are what says it still does.
    ///
    /// A lock that is fenced, or whose release or connection close fails,
    /// cancels `on_failure`, for the process to stop on. The first such
    /// error is the one returned; the other locks are kept until `stop`
    /// all the same.
    pub async fn run<Stop>(
        mut self,
        stop: Stop,
        on_failure: tokio_util::sync::CancellationToken,
    ) -> Result<(), crate::keep_shared_migrations_lock::Error>
    where
        Stop: Future<Output = ()>,
    {
        let mut keepings = futures_util::stream::FuturesUnordered::new();
        let mut first_error = None;
        let mut handovers_left = true;
        let mut stopped = false;

        tokio::pin!(stop);

        loop {
            tokio::select! {
                handed_over = self.receiver.recv(), if handovers_left => {
                    match handed_over {
                        Some(keeping) => keepings.push(keeping),
                        None => handovers_left = false,
                    }
                }

                Some(kept) = keepings.next(), if !keepings.is_empty() => {
                    if let Err(error) = kept {
                        on_failure.cancel();

                        first_error.get_or_insert(error);
                    }
                }

                () = &mut stop, if !stopped => {
                    stopped = true;

                    self.stop.cancel();
                }

                else => break,
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
