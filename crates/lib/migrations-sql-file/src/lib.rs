//! SQL-file migrations: migrations whose run is the content of an SQL
//! file, on a connection through SQLx.

#![warn(missing_docs)]

use derive_where::derive_where;
use waymark_migrations_engine_migration::{
    MigrationMetadata, MigrationRevision, MigrationTransactionSupport, MigrationVersion,
};

pub mod source_checksums;

/// What the expansions of [`migration!`] and of
/// [`source_checksums_test!`] refer to. Not for use by hand.
#[doc(hidden)]
pub mod macro_support {
    pub use envfury;
    pub use waymark_migrations_engine_migration::{
        MigrationRevision, MigrationTransactionSupport, MigrationVersion,
    };
    pub use waymark_migrations_sql_file_macros::{
        migration as migration_at, source_checksums_test as source_checksums_test_at,
    };
}

/// Make an SQL-file migration out of an SQL file, read when the crate is
/// compiled.
///
/// ```ignore
/// waymark_migrations_sql_file::migration!("migrations/0007_add_the_widgets_table.sql")
/// ```
///
/// The argument is the file's path, from the directory of the manifest of
/// the crate the macro is used in, with `/` between its parts. The file is
/// `<version>_<description>.sql`: the version is a number, and the
/// description is the rest of it with its underscores as spaces.
///
/// The file begins with its metadata header: every line down to the first
/// empty one, each an annotation.
///
/// - `-- revision: <revision>`, once: the migration's revision. It is any
///   text that is not empty.
/// - `-- no-transaction`, at most once: the migration cannot run inside a
///   transaction. A file without it can.
///
/// The file is checked against
/// `migrations-engine-sql-files-checksums.json`, the file beside the
/// crate's manifest with the SHA-256 digest of each of its SQL-file
/// migrations. A file that does not match those checksums does not
/// compile, unless `WAYMARK_MIGRATIONS_ALLOW_SOURCE_CHECKSUMS_MISMATCH` or
/// `WAYMARK_MIGRATIONS_UPDATE_SOURCE_CHECKSUMS` is `true` when the crate
/// is compiled: then the migration's revision is the file's followed by
/// ` (source checksums mismatch)`. Neither variable can be `true` in a
/// build without debug assertions. The checksums are checked and written
/// by the test [`source_checksums_test!`] adds to the crate.
///
/// The macro expands to a [`Migration`] with the file's content, its
/// revision and the SHA-256 digest of the content as its source digest,
/// and it is a constant expression. The connection type of the migration
/// is the one its place of use asks for.
///
/// The expansion refers to this crate alone, by the path the macro was
/// reached through: the crate the macro is used in needs no dependency
/// for it beyond the one that path comes from.
#[macro_export]
macro_rules! migration {
    ($path:literal $(,)?) => {
        $crate::macro_support::migration_at!($crate, $path)
    };
}

/// A migration whose run is the content of an SQL file, running through a
/// `Connection`.
///
/// The content is sent to the server as one query string, statements and
/// all. The migration validates nothing before or after its run.
///
/// [`migration!`] makes one out of a file.
#[derive_where(Debug)]
pub struct Migration<Connection> {
    /// The migration's version.
    pub version: MigrationVersion,

    /// A short description of what the migration does.
    pub description: &'static str,

    /// The content of the SQL file.
    pub sql: &'static str,

    /// The migration's revision.
    pub revision: MigrationRevision,

    /// The digest of the content, as a migration's source digest.
    pub source_digest: &'static str,

    /// Whether the migration can run inside a transaction.
    pub transaction_support: MigrationTransactionSupport,

    /// The connection type the migration runs through: the one of the
    /// database the SQL file is written for.
    pub connection_type: core::marker::PhantomData<fn(&mut Connection)>,
}

impl<Connection> waymark_migrations_engine_migration::HasConnection for Migration<Connection> {
    type Connection = Connection;
}

impl<Connection> waymark_migrations_engine_migration::HasMigrationMetadata
    for Migration<Connection>
{
    fn migration_metadata(&self) -> MigrationMetadata<'_> {
        MigrationMetadata {
            version: self.version,
            description: self.description,
            revision: &self.revision,
            source_digest: self.source_digest,
        }
    }
}

impl<Connection> waymark_migrations_engine_migration::HasMigrationTransactionSupport
    for Migration<Connection>
{
    fn migration_transaction_support(&self) -> MigrationTransactionSupport {
        match self.transaction_support {
            MigrationTransactionSupport::Supported => MigrationTransactionSupport::Supported,
            MigrationTransactionSupport::Unsupported => MigrationTransactionSupport::Unsupported,
        }
    }
}

impl<Connection> waymark_migrations_engine_migration::PreValidateMigration
    for Migration<Connection>
{
    type Error = core::convert::Infallible;

    fn pre_validate_migration<'a>(
        &'a self,
        _connection: &'a mut Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

impl<Connection> waymark_migrations_engine_migration::RunMigration for Migration<Connection>
where
    for<'c> &'c mut Connection: sqlx::Executor<'c>,
    Connection: Send,
{
    type Error = sqlx::Error;

    async fn run_migration(&self, connection: &mut Connection) -> Result<(), Self::Error> {
        sqlx::raw_sql(self.sql).execute(connection).await?;

        Ok(())
    }
}

impl<Connection> waymark_migrations_engine_migration::PostValidateMigration
    for Migration<Connection>
{
    type Error = core::convert::Infallible;

    fn post_validate_migration<'a>(
        &'a self,
        _connection: &'a mut Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;
