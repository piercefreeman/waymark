//! The migrations lock keeper: takes a migrations lock, keeps it renewed,
//! converts it and releases it.

use tokio_util::sync::CancellationToken;
use waymark_migrations_engine_backend::migrations_lock;

use crate::ValidMigrationsLockTiming;

/// The exclusive migrations lock, as the holder that took it has it.
///
/// A fenced lock must not be relied on any more: its keeper counted it as
/// lapsed (the time the holder relies on it for passed with no confirmed
/// renewal), a renewal or a conversion found the holder without it, or it
/// was released. Fencing is latched: a fenced lock stays fenced.
#[derive(Debug)]
pub struct ExclusiveMigrationsLock {
    /// Cancelled when the lock is fenced.
    fence: CancellationToken,
}

impl ExclusiveMigrationsLock {
    /// Whether the lock is fenced.
    pub fn is_migrations_lock_fenced(&self) -> bool {
        self.fence.is_cancelled()
    }

    /// Wait for the lock to be fenced.
    ///
    /// Cancellation-safe: fencing is latched, so the future can be dropped
    /// and made again freely.
    pub async fn migrations_lock_fenced(&self) {
        self.fence.cancelled().await;
    }
}

#[cfg(test)]
impl ExclusiveMigrationsLock {
    /// An exclusive lock no keeper took, for the tests of what runs under
    /// one, with the token that fences it.
    pub fn with_fence_for_tests() -> (Self, CancellationToken) {
        let fence = CancellationToken::new();
        let lock = Self {
            fence: fence.clone(),
        };

        (lock, fence)
    }
}

/// A shared migrations lock, as the holder that took it has it.
///
/// A fenced lock must not be relied on any more: its keeper counted it as
/// lapsed (the time the holder relies on it for passed with no confirmed
/// renewal), a renewal found the holder without it, or it was released.
/// Fencing is latched: a fenced lock stays fenced.
#[derive(Debug)]
pub struct SharedMigrationsLock {
    /// Cancelled when the lock is fenced.
    fence: CancellationToken,
}

impl SharedMigrationsLock {
    /// Whether the lock is fenced.
    pub fn is_migrations_lock_fenced(&self) -> bool {
        self.fence.is_cancelled()
    }

    /// Wait for the lock to be fenced.
    ///
    /// Cancellation-safe: fencing is latched, so the future can be dropped
    /// and made again freely.
    pub async fn migrations_lock_fenced(&self) {
        self.fence.cancelled().await;
    }
}

/// The keeper of one holder's migrations lock, through a backend of its
/// own.
///
/// It counts the lock's expiry by its own clock, from the instant before
/// each take, renewal or conversion was sent, and fences the lock when the
/// time the holder relies on it for has passed with no confirmed renewal.
#[derive(Debug)]
pub struct MigrationsLockKeeper<Backend> {
    /// The backend the lock is kept through.
    backend: Backend,

    /// The holder the lock belongs to.
    holder_id: migrations_lock::HolderId,

    /// The lock's timing.
    timing: ValidMigrationsLockTiming,

    /// The instant the keeper counts the lock's confirmed expiry from.
    counted_from: tokio::time::Instant,

    /// Cancelled when the lock is fenced.
    fence: CancellationToken,
}

/// Error returned by [`MigrationsLockKeeper::take_exclusive`].
#[derive(Debug, thiserror::Error)]
pub enum TakeExclusiveError<TakeError, FindError> {
    /// The backend failed to take the lock.
    #[error("taking the exclusive migrations lock: {0}")]
    Take(#[source] TakeError),

    /// The lock was refused, and another holder had the exclusive
    /// migrations lock right after.
    #[error("a migration is in progress: another holder has the exclusive migrations lock")]
    MigrationInProgress,

    /// The lock was refused, and no holder had the exclusive migrations
    /// lock right after: shared ones refused it.
    #[error("the database is in use: other holders have shared migrations locks")]
    DatabaseInUse,

    /// The lock was refused, and the backend failed to tell what refused
    /// it.
    #[error("finding out what refused the exclusive migrations lock: {0}")]
    Find(#[source] FindError),
}

/// Error returned by [`MigrationsLockKeeper::take_shared`].
#[derive(Debug, thiserror::Error)]
pub enum TakeSharedError<TakeError> {
    /// The backend failed to take the lock.
    #[error("taking a shared migrations lock: {0}")]
    Take(#[source] TakeError),

    /// The lock was refused: another holder has the exclusive migrations
    /// lock.
    #[error("a migration is in progress: another holder has the exclusive migrations lock")]
    MigrationInProgress,
}

/// Why [`MigrationsLockKeeper::keep`] returned.
#[derive(Debug)]
pub enum KeepOutcome {
    /// The stop it was given completed. The stop is looked at before the
    /// time the lock is relied on for, so this is the outcome when both
    /// have passed too: the call did not fence the lock, and does not say
    /// that the lock is still the holder's.
    Stopped,

    /// The time the holder relies on the lock for passed with no confirmed
    /// renewal. The lock is fenced.
    Lapsed,

    /// A renewal found the holder without a lock. The lock is fenced.
    Lost,
}

/// Error returned by [`MigrationsLockKeeper::convert_to_shared`].
#[derive(Debug, thiserror::Error)]
pub enum ConvertToSharedError<ConvertError> {
    /// The exclusive lock was fenced before the conversion was sent.
    #[error("the exclusive migrations lock is fenced")]
    Fenced,

    /// The backend failed to convert the lock.
    #[error("converting the exclusive migrations lock to a shared one: {0}")]
    Convert(#[source] ConvertError),

    /// The conversion found the holder without the exclusive lock. The
    /// lock is fenced.
    #[error("the exclusive migrations lock is lost")]
    Lost,
}

/// What [`MigrationsLockKeeper::convert_to_shared`] returns.
#[derive(Debug)]
pub struct ConvertedToShared<SharedBackend, ExclusiveBackend> {
    /// The keeper of the shared lock.
    pub keeper: MigrationsLockKeeper<SharedBackend>,

    /// The shared lock.
    pub lock: SharedMigrationsLock,

    /// The backend the exclusive lock was kept through.
    pub exclusive_backend: ExclusiveBackend,
}

impl<Backend> MigrationsLockKeeper<Backend>
where
    Backend: waymark_migrations_engine_backend::TakeExclusiveMigrationsLock,
    Backend: waymark_migrations_engine_backend::FindExclusiveMigrationsLock,
{
    /// Take the exclusive migrations lock for `holder_id` through
    /// `backend`.
    ///
    /// What refused a refused lock is looked up after the refusal, so it
    /// is what was there then, not at the refusal itself.
    pub async fn take_exclusive(
        mut backend: Backend,
        holder_id: migrations_lock::HolderId,
        timing: ValidMigrationsLockTiming,
    ) -> Result<
        (Self, ExclusiveMigrationsLock),
        TakeExclusiveError<
            <Backend as waymark_migrations_engine_backend::TakeExclusiveMigrationsLock>::Error,
            <Backend as waymark_migrations_engine_backend::FindExclusiveMigrationsLock>::Error,
        >,
    > {
        let counted_from = tokio::time::Instant::now();
        let expiry = migrations_lock::Expiry {
            now: counted_from.into_std(),
            time_to_live: timing.time_to_live(),
        };

        let taken = backend
            .take_exclusive_migrations_lock(&holder_id, expiry)
            .await
            .map_err(TakeExclusiveError::Take)?;

        match taken {
            waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Taken => {}
            waymark_migrations_engine_backend::take_exclusive_migrations_lock::Outcome::Refused => {
                let found = backend
                    .find_exclusive_migrations_lock()
                    .await
                    .map_err(TakeExclusiveError::Find)?;

                return Err(match found {
                    waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::Found => {
                        TakeExclusiveError::MigrationInProgress
                    }
                    waymark_migrations_engine_backend::find_exclusive_migrations_lock::Outcome::NotFound => {
                        TakeExclusiveError::DatabaseInUse
                    }
                });
            }
        }

        let fence = CancellationToken::new();
        let lock = ExclusiveMigrationsLock {
            fence: fence.clone(),
        };
        let keeper = Self {
            backend,
            holder_id,
            timing,
            counted_from,
            fence,
        };

        Ok((keeper, lock))
    }
}

impl<Backend> MigrationsLockKeeper<Backend>
where
    Backend: waymark_migrations_engine_backend::TakeSharedMigrationsLock,
{
    /// Take a shared migrations lock for `holder_id` through `backend`.
    pub async fn take_shared(
        mut backend: Backend,
        holder_id: migrations_lock::HolderId,
        timing: ValidMigrationsLockTiming,
    ) -> Result<
        (Self, SharedMigrationsLock),
        TakeSharedError<
            <Backend as waymark_migrations_engine_backend::TakeSharedMigrationsLock>::Error,
        >,
    > {
        let counted_from = tokio::time::Instant::now();
        let expiry = migrations_lock::Expiry {
            now: counted_from.into_std(),
            time_to_live: timing.time_to_live(),
        };

        let taken = backend
            .take_shared_migrations_lock(&holder_id, expiry)
            .await
            .map_err(TakeSharedError::Take)?;

        match taken {
            waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Taken => {}
            waymark_migrations_engine_backend::take_shared_migrations_lock::Outcome::Refused => {
                return Err(TakeSharedError::MigrationInProgress);
            }
        }

        let fence = CancellationToken::new();
        let lock = SharedMigrationsLock {
            fence: fence.clone(),
        };
        let keeper = Self {
            backend,
            holder_id,
            timing,
            counted_from,
            fence,
        };

        Ok((keeper, lock))
    }
}

impl<Backend> MigrationsLockKeeper<Backend>
where
    Backend: waymark_migrations_engine_backend::RenewMigrationsLock,
    <Backend as waymark_migrations_engine_backend::RenewMigrationsLock>::Error: std::fmt::Display,
{
    /// Keep the lock renewed, one renewal a heartbeat after the one
    /// before, until `stop` completes, the lock lapses or the lock is
    /// lost.
    ///
    /// A renewal that fails is logged and not sent again: the next one
    /// goes out at its own heartbeat. A renewal on its way is waited for,
    /// never dropped: `stop` is looked at between renewals only, and a
    /// lock that lapses while a renewal is on its way is fenced at once
    /// and reported once that renewal returned.
    pub async fn keep<Stop>(&mut self, stop: Stop) -> KeepOutcome
    where
        Stop: Future<Output = ()>,
    {
        let heartbeat = self.timing.heartbeat().get();

        let mut next_renewal_at = self.counted_from + heartbeat;

        tokio::pin!(stop);

        loop {
            let lapses_at = self.counted_from + self.timing.relied_on_for();

            tokio::select! {
                biased;

                () = &mut stop => return KeepOutcome::Stopped,

                () = tokio::time::sleep_until(lapses_at) => {
                    self.fence.cancel();

                    return KeepOutcome::Lapsed;
                }

                () = tokio::time::sleep_until(next_renewal_at) => {}
            }

            let counted_from = tokio::time::Instant::now();
            let expiry = migrations_lock::Expiry {
                now: counted_from.into_std(),
                time_to_live: self.timing.time_to_live(),
            };

            let renewed = {
                let renewing = self.backend.renew_migrations_lock(&self.holder_id, expiry);

                tokio::pin!(renewing);

                loop {
                    tokio::select! {
                        biased;

                        renewed = &mut renewing => break renewed,

                        () = tokio::time::sleep_until(lapses_at), if !self.fence.is_cancelled() => {
                            self.fence.cancel();
                        }
                    }
                }
            };

            match renewed {
                Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Renewed) => {
                    if self.fence.is_cancelled() {
                        return KeepOutcome::Lapsed;
                    }

                    self.counted_from = counted_from;
                }
                Ok(waymark_migrations_engine_backend::renew_migrations_lock::Outcome::Lost) => {
                    self.fence.cancel();

                    return KeepOutcome::Lost;
                }
                Err(error) => {
                    tracing::warn!(%error, "renewing the migrations lock failed");

                    if self.fence.is_cancelled() {
                        return KeepOutcome::Lapsed;
                    }
                }
            }

            next_renewal_at = counted_from + heartbeat;
        }
    }
}

impl<Backend> MigrationsLockKeeper<Backend> {
    /// The backend the lock is kept through, for what else the holder
    /// does through it while the keeper is not keeping.
    pub fn backend(&mut self) -> &mut Backend {
        &mut self.backend
    }

    /// Turn the exclusive lock into a shared one, sent and kept from then
    /// on through `shared_backend`.
    ///
    /// On an error neither backend is handed back.
    pub async fn convert_to_shared<SharedBackend>(
        self,
        lock: ExclusiveMigrationsLock,
        mut shared_backend: SharedBackend,
    ) -> Result<
        ConvertedToShared<SharedBackend, Backend>,
        ConvertToSharedError<
            <SharedBackend as waymark_migrations_engine_backend::ConvertMigrationsLockToShared>::Error,
        >,
    >
    where
        SharedBackend: waymark_migrations_engine_backend::ConvertMigrationsLockToShared,
{
        let Self {
            backend: exclusive_backend,
            holder_id,
            timing,
            counted_from: exclusive_counted_from,
            fence,
        } = self;

        // The proof of the exclusive lock is given up here: nothing can run
        // under it once the conversion is sent.
        drop(lock);

        let counted_from = tokio::time::Instant::now();

        if counted_from >= exclusive_counted_from + timing.relied_on_for() {
            fence.cancel();
        }

        if fence.is_cancelled() {
            return Err(ConvertToSharedError::Fenced);
        }

        let expiry = migrations_lock::Expiry {
            now: counted_from.into_std(),
            time_to_live: timing.time_to_live(),
        };

        let converted = shared_backend
            .convert_migrations_lock_to_shared(&holder_id, expiry)
            .await
            .map_err(ConvertToSharedError::Convert)?;

        match converted {
            waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Converted => {}
            waymark_migrations_engine_backend::convert_migrations_lock_to_shared::Outcome::Lost => {
                fence.cancel();

                return Err(ConvertToSharedError::Lost);
            }
        }

        let lock = SharedMigrationsLock {
            fence: fence.clone(),
        };
        let keeper = MigrationsLockKeeper {
            backend: shared_backend,
            holder_id,
            timing,
            counted_from,
            fence,
        };

        Ok(ConvertedToShared {
            keeper,
            lock,
            exclusive_backend,
        })
    }
}

impl<Backend> MigrationsLockKeeper<Backend>
where
    Backend: waymark_migrations_engine_backend::ReleaseMigrationsLock,
{
    /// Fence the lock and release it, shared or exclusive, and hand the
    /// backend back with how the release went.
    pub async fn release(
        mut self,
    ) -> (
        Backend,
        Result<(), <Backend as waymark_migrations_engine_backend::ReleaseMigrationsLock>::Error>,
    ) {
        self.fence.cancel();

        let released = self.backend.release_migrations_lock(&self.holder_id).await;

        (self.backend, released)
    }
}

#[cfg(test)]
mod tests;
