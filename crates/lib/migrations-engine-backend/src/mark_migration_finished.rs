//! Migration finish marking trait.

use waymark_migrations_engine_migration::MigrationVersion;

/// Backend capability for marking a started migration as finished.
pub trait MarkMigrationFinished {
    /// An error that can occur while marking; a ledger with no started
    /// entry of the migration is one.
    type Error;

    /// Turn the started ledger entry of the migration of `version` into a
    /// finished one.
    fn mark_migration_finished(
        &mut self,
        version: MigrationVersion,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
