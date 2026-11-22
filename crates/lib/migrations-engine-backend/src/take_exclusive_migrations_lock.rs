//! Exclusive migrations lock taking trait.

use crate::migrations_lock;

/// The outcome of taking the exclusive migrations lock.
#[derive(Debug)]
pub enum Outcome {
    /// The holder has the exclusive migrations lock now.
    Taken,

    /// Another migrations lock that has not expired exists, shared or
    /// exclusive, so the holder got no lock.
    Refused,
}

/// Backend capability for taking the exclusive migrations lock.
pub trait TakeExclusiveMigrationsLock {
    /// An error that can occur while taking the lock; a refusal is not one.
    type Error;

    /// Take the exclusive migrations lock for `holder_id`, expiring as
    /// `expiry` says.
    ///
    /// The lock is refused while any other migrations lock that has not
    /// expired exists, shared or exclusive.
    fn take_exclusive_migrations_lock<'a>(
        &'a mut self,
        holder_id: &'a migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> impl Future<Output = Result<Outcome, Self::Error>> + Send + 'a;
}
