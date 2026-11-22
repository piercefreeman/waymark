//! The whole-run transaction.
//!
//! It goes through the driver's own transaction manager, so the driver
//! knows the connection is inside a transaction: a migration that begins
//! one of its own on the connection then gets a savepoint, and its commit
//! releases that savepoint and not the whole run.

// `sqlx` exports the Postgres transaction manager but not the trait its
// operations are in; the trait comes from `sqlx-core`, the same package
// `sqlx` is built on.
use sqlx_core::transaction::TransactionManager as _;

impl<Tables> waymark_migrations_engine_backend::BeginWholeRunTransaction
    for crate::Backend<Tables>
{
    type Error = sqlx::Error;

    async fn begin_whole_run_transaction(&mut self) -> Result<(), Self::Error> {
        sqlx::postgres::PgTransactionManager::begin(&mut self.connection, None).await
    }
}

impl<Tables> waymark_migrations_engine_backend::CommitWholeRunTransaction
    for crate::Backend<Tables>
{
    type Error = sqlx::Error;

    async fn commit_whole_run_transaction(&mut self) -> Result<(), Self::Error> {
        sqlx::postgres::PgTransactionManager::commit(&mut self.connection).await
    }
}

impl<Tables> waymark_migrations_engine_backend::RollbackWholeRunTransaction
    for crate::Backend<Tables>
{
    type Error = sqlx::Error;

    async fn rollback_whole_run_transaction(&mut self) -> Result<(), Self::Error> {
        sqlx::postgres::PgTransactionManager::rollback(&mut self.connection).await
    }
}
