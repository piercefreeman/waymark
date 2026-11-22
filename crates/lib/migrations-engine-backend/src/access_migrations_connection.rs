//! Migrations connection access trait.

/// Backend capability for giving out the connection the backend holds,
/// for the migrations to run through.
pub trait AccessMigrationsConnection: crate::HasConnection {
    /// The connection the backend holds.
    fn migrations_connection(&mut self) -> &mut Self::Connection;
}
