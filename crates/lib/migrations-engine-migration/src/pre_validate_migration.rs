//! Migration pre-validation trait.

/// Migration capability for validating a database before the migration
/// runs on it.
pub trait PreValidateMigration: crate::HasConnection {
    /// An error that can occur while pre-validating; a database the
    /// migration cannot run on is one.
    type Error;

    /// Validate, through `connection`, that the database is one the
    /// migration can run on.
    fn pre_validate_migration<'a>(
        &'a self,
        connection: &'a mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
