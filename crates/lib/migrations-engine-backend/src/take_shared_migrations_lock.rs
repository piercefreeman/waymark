//! Shared migrations lock taking trait.

use crate::migrations_lock;

/// The outcome of taking a shared migrations lock.
#[derive(Debug)]
pub enum Outcome {
    /// The holder has a shared migrations lock now.
    Taken,

    /// An exclusive migrations lock that has not expired exists, so the
    /// holder got no lock.
    Refused,
}

/// Backend capability for taking a shared migrations lock.
pub trait TakeSharedMigrationsLock {
    /// An error that can occur while taking the lock; a refusal is not one.
    type Error;

    /// Take a shared migrations lock for `holder_id`, expiring as `expiry`
    /// says.
    ///
    /// The lock is refused while an exclusive migrations lock that has not
    /// expired exists; shared migrations locks do not refuse one another.
    fn take_shared_migrations_lock<'a>(
        &'a mut self,
        holder_id: &'a migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> impl Future<Output = Result<Outcome, Self::Error>> + Send + 'a;
}
