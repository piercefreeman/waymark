//! Migrations lock conversion trait.

use crate::migrations_lock;

/// The outcome of converting the exclusive migrations lock to a shared
/// one.
#[derive(Debug)]
pub enum Outcome {
    /// The holder's exclusive migrations lock is a shared one now.
    Converted,

    /// The holder has no exclusive migrations lock any more.
    Lost,
}

/// Backend capability for converting the exclusive migrations lock to a
/// shared one.
pub trait ConvertMigrationsLockToShared {
    /// An error that can occur while converting; a lost lock is not one.
    type Error;

    /// Turn the exclusive migrations lock of `holder_id` into a shared
    /// one, expiring as `expiry` says. The holder is at no moment without
    /// a lock in between.
    fn convert_migrations_lock_to_shared<'a>(
        &'a mut self,
        holder_id: &'a migrations_lock::HolderId,
        expiry: migrations_lock::Expiry,
    ) -> impl Future<Output = Result<Outcome, Self::Error>> + Send + 'a;
}
