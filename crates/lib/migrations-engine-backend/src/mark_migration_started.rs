//! Migration start marking trait.

use waymark_migrations_engine_migration::MigrationMetadata;

/// Backend capability for marking a migration as started.
pub trait MarkMigrationStarted {
    /// An error that can occur while marking.
    type Error;

    /// Write the ledger entry of the migration with `metadata`, as
    /// started.
    fn mark_migration_started<'a>(
        &'a mut self,
        metadata: MigrationMetadata<'a>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
