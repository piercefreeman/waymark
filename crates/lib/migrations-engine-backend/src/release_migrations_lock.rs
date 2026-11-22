//! Migrations lock release trait.

use crate::migrations_lock;

/// Backend capability for releasing a migrations lock.
pub trait ReleaseMigrationsLock {
    /// An error that can occur while releasing.
    type Error;

    /// Release the migrations lock of `holder_id`, shared or exclusive. A
    /// holder with no lock any more is released already, which is not an
    /// error.
    fn release_migrations_lock<'a>(
        &'a mut self,
        holder_id: &'a migrations_lock::HolderId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
