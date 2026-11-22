//! Migration run trait.

/// Migration capability for running the migration on a database.
pub trait RunMigration: crate::HasConnection {
    /// An error that can occur while running.
    type Error;

    /// Run the migration through `connection`.
    fn run_migration<'a>(
        &'a self,
        connection: &'a mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
