//! Rust-code migrations: migrations whose pre-validation, run and
//! post-validation are Rust code.

#![warn(missing_docs)]

use std::pin::Pin;

use waymark_migrations_engine_migration::{MigrationIdentity, MigrationTransactionSupport};

pub mod source_checksums;

pub use waymark_migrations_rust_code_macros::{migration, migration_identity};

/// What the expansion of [`migration_identity!`] refers to. Not for use
/// by hand.
#[doc(hidden)]
pub mod macro_support {
    pub use envfury;
}

/// The error of an operation of a [`Migration`]: the error of the
/// migration behind it, with its type erased.
///
/// It shows as that error does, and its source is that error's source.
#[derive(Debug)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);

impl Error {
    /// The error of the migration behind, for telling its type.
    pub fn inner(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        &*self.0
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// The future of an operation with its error's type erased.
type ErasedOperation<'a> = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;

/// A migration with the futures of its operations boxed and their errors'
/// types erased, so that migrations of different types are one type.
trait ErasedMigration<Connection>: Send + Sync {
    /// The migration's identity.
    fn migration_identity(&self) -> MigrationIdentity<'_>;

    /// Whether the migration can run inside a transaction.
    fn migration_transaction_support(&self) -> MigrationTransactionSupport;

    /// The pre-validation.
    fn pre_validate_migration<'a>(&'a self, connection: &'a mut Connection) -> ErasedOperation<'a>;

    /// The run.
    fn run_migration<'a>(&'a self, connection: &'a mut Connection) -> ErasedOperation<'a>;

    /// The post-validation.
    fn post_validate_migration<'a>(&'a self, connection: &'a mut Connection)
    -> ErasedOperation<'a>;
}

impl<Connection, Migration> ErasedMigration<Connection> for Migration
where
    Migration: waymark_migrations_engine_migration::Migration<Connection = Connection>,
    <Migration as waymark_migrations_engine_migration::PreValidateMigration>::Error:
        std::error::Error + Send + Sync + 'static,
    <Migration as waymark_migrations_engine_migration::RunMigration>::Error:
        std::error::Error + Send + Sync + 'static,
    <Migration as waymark_migrations_engine_migration::PostValidateMigration>::Error:
        std::error::Error + Send + Sync + 'static,
    Migration: Send + Sync,
{
    fn migration_identity(&self) -> MigrationIdentity<'_> {
        waymark_migrations_engine_migration::HasMigrationIdentity::migration_identity(self)
    }

    fn migration_transaction_support(&self) -> MigrationTransactionSupport {
        waymark_migrations_engine_migration::HasMigrationTransactionSupport::migration_transaction_support(self)
    }

    fn pre_validate_migration<'a>(&'a self, connection: &'a mut Connection) -> ErasedOperation<'a> {
        let pre_validating =
            waymark_migrations_engine_migration::PreValidateMigration::pre_validate_migration(
                self, connection,
            );

        Box::pin(async move { pre_validating.await.map_err(|error| Error(Box::new(error))) })
    }

    fn run_migration<'a>(&'a self, connection: &'a mut Connection) -> ErasedOperation<'a> {
        let running =
            waymark_migrations_engine_migration::RunMigration::run_migration(self, connection);

        Box::pin(async move { running.await.map_err(|error| Error(Box::new(error))) })
    }

    fn post_validate_migration<'a>(
        &'a self,
        connection: &'a mut Connection,
    ) -> ErasedOperation<'a> {
        let post_validating =
            waymark_migrations_engine_migration::PostValidateMigration::post_validate_migration(
                self, connection,
            );

        Box::pin(async move {
            post_validating
                .await
                .map_err(|error| Error(Box::new(error)))
        })
    }
}

/// A migration running through a `Connection`, with its type erased:
/// Rust-code migrations of different types are this one type.
///
/// [`migration!`] declares the migration a crate is and gives it as one
/// of these.
pub struct Migration<Connection> {
    /// The migration behind.
    migration: Box<dyn ErasedMigration<Connection>>,
}

impl<Connection> Migration<Connection> {
    /// `migration` with its type erased.
    pub fn new<Migration>(migration: Migration) -> Self
    where
        Migration: waymark_migrations_engine_migration::Migration<Connection = Connection>,
        <Migration as waymark_migrations_engine_migration::PreValidateMigration>::Error:
            std::error::Error + Send + Sync + 'static,
        <Migration as waymark_migrations_engine_migration::RunMigration>::Error:
            std::error::Error + Send + Sync + 'static,
        <Migration as waymark_migrations_engine_migration::PostValidateMigration>::Error:
            std::error::Error + Send + Sync + 'static,
        Migration: Send + Sync + 'static,
    {
        Self {
            migration: Box::new(migration),
        }
    }
}

impl<Connection> std::fmt::Debug for Migration<Connection> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Migration")
            .field("identity", &self.migration.migration_identity())
            .field(
                "transaction_support",
                &self.migration.migration_transaction_support(),
            )
            .finish_non_exhaustive()
    }
}

impl<Connection> waymark_migrations_engine_migration::HasConnection for Migration<Connection> {
    type Connection = Connection;
}

impl<Connection> waymark_migrations_engine_migration::HasMigrationIdentity
    for Migration<Connection>
{
    fn migration_identity(&self) -> MigrationIdentity<'_> {
        self.migration.migration_identity()
    }
}

impl<Connection> waymark_migrations_engine_migration::HasMigrationTransactionSupport
    for Migration<Connection>
{
    fn migration_transaction_support(&self) -> MigrationTransactionSupport {
        self.migration.migration_transaction_support()
    }
}

impl<Connection> waymark_migrations_engine_migration::PreValidateMigration
    for Migration<Connection>
{
    type Error = Error;

    fn pre_validate_migration<'a>(
        &'a self,
        connection: &'a mut Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.migration.pre_validate_migration(connection)
    }
}

impl<Connection> waymark_migrations_engine_migration::RunMigration for Migration<Connection> {
    type Error = Error;

    fn run_migration<'a>(
        &'a self,
        connection: &'a mut Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.migration.run_migration(connection)
    }
}

impl<Connection> waymark_migrations_engine_migration::PostValidateMigration
    for Migration<Connection>
{
    type Error = Error;

    fn post_validate_migration<'a>(
        &'a self,
        connection: &'a mut Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        self.migration.post_validate_migration(connection)
    }
}

#[cfg(test)]
mod tests;
