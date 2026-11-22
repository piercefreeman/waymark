//! Migrations lock renewal trait.

use crate::migrations_lock;

/// The outcome of renewing a migrations lock.
#[derive(Debug)]
pub enum Outcome {
    /// The holder's migrations lock has its new expiry.
    Renewed,

    /// The holder has no migrations lock any more.
    Lost,
}

/// Backend capability for renewing a migrations lock.
pub trait RenewMigrationsLock {
    /// An error that can occur while renewing; a lost lock is not one.
    type Error;

    /// Move the expiry of the migrations lock of `holder_id`, shared or
    /// exclusive, to what `expiry` says.
    fn renew_migrations_lock<'a>(
        &'a mut self,
        holder_id: &'a migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> impl Future<Output = Result<Outcome, Self::Error>> + Send + 'a;
}
