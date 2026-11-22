//! Migrations engine setup trait.

/// Backend capability for setting the migrations engine up in a database.
pub trait SetupMigrationsEngine {
    /// An error that can occur while setting up.
    type Error;

    /// Create what the migrations engine keeps in the database: the
    /// migrations ledger and the migrations locks. What is there already
    /// stays as it is.
    ///
    /// Any number of backends may set the same database up at once.
    fn setup_migrations_engine(
        &mut self,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
