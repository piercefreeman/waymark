//! Migration post-validation trait.

/// Migration capability for validating a database after the migration ran
/// on it.
pub trait PostValidateMigration: crate::HasConnection {
    /// An error that can occur while post-validating; a database the
    /// migration did not leave as it should have is one.
    type Error;

    /// Validate, through `connection`, that the migration left the
    /// database as it should have.
    fn post_validate_migration<'a>(
        &'a self,
        connection: &'a mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
