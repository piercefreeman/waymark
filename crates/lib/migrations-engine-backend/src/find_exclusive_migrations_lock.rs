//! Exclusive migrations lock finding trait.

/// The outcome of looking for the exclusive migrations lock.
#[derive(Debug)]
pub enum Outcome {
    /// An exclusive migrations lock that has not expired exists.
    Found,

    /// No exclusive migrations lock exists, or the one that does has
    /// expired.
    NotFound,
}

/// Backend capability for finding out whether the exclusive migrations
/// lock is taken.
pub trait FindExclusiveMigrationsLock {
    /// An error that can occur while looking.
    type Error;

    /// Look for an exclusive migrations lock that has not expired, of any
    /// holder. Nothing is written.
    fn find_exclusive_migrations_lock(
        &mut self,
    ) -> impl Future<Output = Result<Outcome, Self::Error>> + Send + '_;
}
