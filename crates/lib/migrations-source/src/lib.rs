//! The migration source: an SQL-file migration or a Rust-code migration.

#![warn(missing_docs)]

// The test of the macro gives it this crate as the crate of a Rust-code
// migration: the crate has to resolve by its name, with a `migration()`
// at its root.
#[cfg(test)]
extern crate self as waymark_migrations_source;

use derive_where::derive_where;
use waymark_migrations_engine_migration::{MigrationIdentity, MigrationTransactionSupport};

#[cfg(test)]
use self::tests::migration;

/// A migration running through a `Connection`, from one of the two
/// sources: an SQL file or Rust code.
///
/// Implements each trait of the migration interface through the migration
/// it holds.
///
/// [`migrations!`] makes an array of these.
#[derive_where(Debug)]
pub enum MigrationSource<Connection> {
    /// An SQL-file migration.
    SqlFile(waymark_migrations_sql_file::Migration<Connection>),

    /// A Rust-code migration.
    RustCode(waymark_migrations_rust_code::Migration<Connection>),
}

/// Error of an operation of a [`MigrationSource`]: the error of the
/// migration it holds.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The operation of the SQL-file migration failed.
    #[error("sql file migration: {0}")]
    SqlFile(#[source] sqlx::Error),

    /// The operation of the Rust-code migration failed.
    #[error("rust code migration: {0}")]
    RustCode(#[source] waymark_migrations_rust_code::Error),
}

/// Make a migrations array out of its migrations, in the order they are
/// written in.
///
/// ```ignore
/// waymark_migrations_source::migrations![
///     "migrations/0001_init.sql",
///     "migrations/0002_add_the_widgets_table.sql",
///     waymark_backend_postgres_migration_0027_id_map_tables,
/// ]
/// ```
///
/// A string is the path of an SQL file, as
/// `waymark_migrations_sql_file::migration!` takes it. An identifier is
/// the crate of a Rust-code migration, as
/// `waymark_migrations_rust_code::migration!` declares one.
///
/// The macro expands to an array of [`MigrationSource`]. The crate it is
/// used in depends on the crates of its Rust-code migrations; its SQL-file
/// migrations need no dependency of their own.
#[macro_export]
macro_rules! migrations {
    ($($migration:tt),* $(,)?) => {
        [$($crate::migration_source!($migration)),*]
    };
}

/// One migration of [`migrations!`].
#[doc(hidden)]
#[macro_export]
macro_rules! migration_source {
    ($sql_file:literal) => {
        $crate::MigrationSource::SqlFile($crate::macro_support::migration!($sql_file))
    };
    ($rust_code_migration_crate:ident) => {
        $crate::MigrationSource::RustCode(::$rust_code_migration_crate::migration())
    };
}

/// Add to a crate with a migrations array the tests of its source
/// checksums.
///
/// ```ignore
/// waymark_migrations_source::source_checksums_tests!(sql_files("migrations"));
/// ```
///
/// What is inside `sql_files(…)` goes, as it is, to
/// `waymark_migrations_sql_file::source_checksums_test!`, which adds the
/// test of the crate's SQL files checksums against its SQL-file
/// migrations.
///
/// A Rust-code migration has the test of its crate capture in its own
/// crate.
#[macro_export]
macro_rules! source_checksums_tests {
    (sql_files($($sql_files:tt)*) $(,)?) => {
        $crate::macro_support::source_checksums_test!($($sql_files)*);
    };
}

/// What the expansions of [`migrations!`] and of
/// [`source_checksums_tests!`] refer to. Not for use by hand.
#[doc(hidden)]
pub mod macro_support {
    pub use waymark_migrations_sql_file::{migration, source_checksums_test};
}

impl<Connection> waymark_migrations_engine_migration::HasConnection
    for MigrationSource<Connection>
{
    type Connection = Connection;
}

impl<Connection> waymark_migrations_engine_migration::HasMigrationIdentity
    for MigrationSource<Connection>
{
    fn migration_identity(&self) -> MigrationIdentity<'_> {
        match self {
            Self::SqlFile(migration) => {
                waymark_migrations_engine_migration::HasMigrationIdentity::migration_identity(
                    migration,
                )
            }
            Self::RustCode(migration) => {
                waymark_migrations_engine_migration::HasMigrationIdentity::migration_identity(
                    migration,
                )
            }
        }
    }
}

impl<Connection> waymark_migrations_engine_migration::HasMigrationTransactionSupport
    for MigrationSource<Connection>
{
    fn migration_transaction_support(&self) -> MigrationTransactionSupport {
        match self {
            Self::SqlFile(migration) => {
                waymark_migrations_engine_migration::HasMigrationTransactionSupport::migration_transaction_support(
                    migration,
                )
            }
            Self::RustCode(migration) => {
                waymark_migrations_engine_migration::HasMigrationTransactionSupport::migration_transaction_support(
                    migration,
                )
            }
        }
    }
}

impl<Connection> waymark_migrations_engine_migration::PreValidateMigration
    for MigrationSource<Connection>
where
    Connection: Send,
{
    type Error = Error;

    async fn pre_validate_migration(&self, connection: &mut Connection) -> Result<(), Self::Error> {
        match self {
            Self::SqlFile(migration) => {
                let Ok(()) =
                    waymark_migrations_engine_migration::PreValidateMigration::pre_validate_migration(
                        migration, connection,
                    )
                    .await;

                Ok(())
            }
            Self::RustCode(migration) => {
                waymark_migrations_engine_migration::PreValidateMigration::pre_validate_migration(
                    migration, connection,
                )
                .await
                .map_err(Error::RustCode)
            }
        }
    }
}

impl<Connection> waymark_migrations_engine_migration::RunMigration for MigrationSource<Connection>
where
    for<'c> &'c mut Connection: sqlx::Executor<'c>,
    Connection: Send,
{
    type Error = Error;

    async fn run_migration(&self, connection: &mut Connection) -> Result<(), Self::Error> {
        match self {
            Self::SqlFile(migration) => {
                waymark_migrations_engine_migration::RunMigration::run_migration(
                    migration, connection,
                )
                .await
                .map_err(Error::SqlFile)
            }
            Self::RustCode(migration) => {
                waymark_migrations_engine_migration::RunMigration::run_migration(
                    migration, connection,
                )
                .await
                .map_err(Error::RustCode)
            }
        }
    }
}

impl<Connection> waymark_migrations_engine_migration::PostValidateMigration
    for MigrationSource<Connection>
where
    Connection: Send,
{
    type Error = Error;

    async fn post_validate_migration(
        &self,
        connection: &mut Connection,
    ) -> Result<(), Self::Error> {
        match self {
            Self::SqlFile(migration) => {
                let Ok(()) =
                    waymark_migrations_engine_migration::PostValidateMigration::post_validate_migration(
                        migration, connection,
                    )
                    .await;

                Ok(())
            }
            Self::RustCode(migration) => {
                waymark_migrations_engine_migration::PostValidateMigration::post_validate_migration(
                    migration, connection,
                )
                .await
                .map_err(Error::RustCode)
            }
        }
    }
}

#[cfg(test)]
mod tests;
