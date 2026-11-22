//! Migration 0027 of the main database: the stored VM runtime snapshots
//! move onto the id map tables.
//!
//! A snapshot is one MessagePack value, a runtime state: the sequence of
//! the ready frames, the promise states table, the select states table and
//! the effect counter. The migration rewrites the two tables of every
//! `vm_runtime_snapshots.snapshot`:
//!
//! | Table | Before | After |
//! | --- | --- | --- |
//! | promise states | `[state, …]`, each state's id its position | `[next id, {id: state, …}]` |
//! | select states | `[next id, {id: continuation, …}]` | `[[next id, {id: continuation, …}]]` |
//!
//! Everything else in a snapshot passes through as it is.
//!
//! The run reads the snapshots in batches of a fixed size, in the order of
//! their VM ids, and writes each batch back in one statement: two
//! statements per batch, and one batch of snapshots in memory, in both
//! shapes. It runs in a transaction of its own — a savepoint inside the
//! whole-run transaction — so the snapshots are rewritten all together or
//! not at all.
//!
//! The post-validation reads every snapshot again and checks that it is in
//! the shape after the migration.
//!
//! The crate is frozen with the migration: both shapes are written down
//! here, and nothing follows the runtime's own types.

#![warn(missing_docs)]

mod snapshot_rewrite;
#[cfg(test)]
mod test_fixtures;

use self::snapshot_rewrite::{
    CheckRewrittenSnapshotError, RewriteSnapshotError, check_rewritten_snapshot, rewrite_snapshot,
};

/// How many snapshots are read, rewritten and written back at a time: the
/// most the migration has in memory at once, in both shapes.
const SNAPSHOTS_PER_BATCH: i64 = 64;

waymark_migrations_rust_code::migration! {
    revision: "id-map-tables-1",
    connection_type: sqlx::PgConnection,
    transaction_support:
        waymark_migrations_engine_migration::MigrationTransactionSupport::Supported,
}

/// Error of the migration's run.
#[derive(Debug, thiserror::Error)]
enum RunError {
    /// The run's own transaction could not be begun.
    #[error("beginning the transaction of the rewrite: {0}")]
    Begin(#[source] sqlx::Error),

    /// The snapshots were not rewritten; the transaction was rolled back.
    #[error("rewriting the snapshots: {0}")]
    Rewrite(#[source] RewriteSnapshotsError),

    /// The snapshots were not rewritten, and the transaction could not be
    /// rolled back either.
    #[error(
        "rewriting the snapshots: {rewrite}; rolling the transaction of the rewrite back: \
         {rollback}"
    )]
    RewriteAndRollback {
        /// Why the snapshots were not rewritten.
        rewrite: RewriteSnapshotsError,

        /// Why the transaction was not rolled back.
        #[source]
        rollback: sqlx::Error,
    },

    /// The run's own transaction could not be committed.
    #[error("committing the transaction of the rewrite: {0}")]
    Commit(#[source] sqlx::Error),
}

/// Error of rewriting the stored snapshots.
#[derive(Debug, thiserror::Error)]
enum RewriteSnapshotsError {
    /// A batch of snapshots could not be read.
    #[error("reading a batch of snapshots: {0}")]
    ReadBatch(#[source] sqlx::Error),

    /// A snapshot is not in the shape before the migration.
    #[error("the snapshot of VM {vm_id}: {error}")]
    RewriteSnapshot {
        /// The VM of the snapshot.
        vm_id: uuid::Uuid,

        /// What is off about the snapshot.
        #[source]
        error: RewriteSnapshotError,
    },

    /// A batch of rewritten snapshots could not be written.
    #[error("writing a batch of rewritten snapshots: {0}")]
    WriteBatch(#[source] sqlx::Error),

    /// Not every snapshot of a batch was there to be written back.
    #[error("{written} of the {read} snapshots of a batch were written back")]
    BatchWrittenPartly {
        /// How many snapshots the batch read.
        read: usize,

        /// How many rows the batch's write changed.
        written: u64,
    },
}

/// Error of the migration's post-validation.
#[derive(Debug, thiserror::Error)]
enum PostValidateError {
    /// A batch of snapshots could not be read.
    #[error("reading a batch of snapshots: {0}")]
    ReadBatch(#[source] sqlx::Error),

    /// A snapshot is not in the shape after the migration.
    #[error("the snapshot of VM {vm_id}: {error}")]
    CheckSnapshot {
        /// The VM of the snapshot.
        vm_id: uuid::Uuid,

        /// What is off about the snapshot.
        #[source]
        error: CheckRewrittenSnapshotError,
    },
}

/// Read the batch of snapshots after the VM `after`, or the first batch
/// when `after` is `None`, in the order of the VM ids.
async fn read_batch(
    connection: &mut sqlx::PgConnection,
    after: Option<uuid::Uuid>,
) -> Result<Vec<(uuid::Uuid, Vec<u8>)>, sqlx::Error> {
    match after {
        None => {
            sqlx::query_as(
                r#"
SELECT vm_id, snapshot
FROM vm_runtime_snapshots
ORDER BY vm_id
LIMIT $1
                "#,
            )
            .bind(SNAPSHOTS_PER_BATCH)
            .fetch_all(connection)
            .await
        }
        Some(after) => {
            sqlx::query_as(
                r#"
SELECT vm_id, snapshot
FROM vm_runtime_snapshots
WHERE vm_id > $1
ORDER BY vm_id
LIMIT $2
                "#,
            )
            .bind(after)
            .bind(SNAPSHOTS_PER_BATCH)
            .fetch_all(connection)
            .await
        }
    }
}

/// Rewrite every stored snapshot, batch after batch.
async fn rewrite_snapshots(
    connection: &mut sqlx::PgConnection,
) -> Result<(), RewriteSnapshotsError> {
    let mut after = None;

    loop {
        let batch = read_batch(connection, after)
            .await
            .map_err(RewriteSnapshotsError::ReadBatch)?;

        let Some((last_vm_id, _snapshot)) = batch.last() else {
            return Ok(());
        };
        after = Some(*last_vm_id);

        let mut vm_ids = Vec::with_capacity(batch.len());
        let mut rewritten_snapshots = Vec::with_capacity(batch.len());

        for (vm_id, snapshot) in &batch {
            let rewritten = rewrite_snapshot(snapshot).map_err(|error| {
                RewriteSnapshotsError::RewriteSnapshot {
                    vm_id: *vm_id,
                    error,
                }
            })?;

            vm_ids.push(*vm_id);
            rewritten_snapshots.push(rewritten);
        }

        let written = sqlx::query(
            r#"
UPDATE vm_runtime_snapshots AS stored
SET snapshot = rewritten.snapshot
FROM UNNEST($1::uuid[], $2::bytea[]) AS rewritten(vm_id, snapshot)
WHERE stored.vm_id = rewritten.vm_id
            "#,
        )
        .bind(&vm_ids)
        .bind(&rewritten_snapshots)
        .execute(&mut *connection)
        .await
        .map_err(RewriteSnapshotsError::WriteBatch)?;

        let all_written =
            u64::try_from(batch.len()).is_ok_and(|read| read == written.rows_affected());

        if !all_written {
            return Err(RewriteSnapshotsError::BatchWrittenPartly {
                read: batch.len(),
                written: written.rows_affected(),
            });
        }
    }
}

/// The migration's run: every stored snapshot rewritten, in a transaction
/// of its own.
async fn run(connection: &mut sqlx::PgConnection) -> Result<(), RunError> {
    let mut transaction = sqlx::Connection::begin(connection)
        .await
        .map_err(RunError::Begin)?;

    let rewritten = rewrite_snapshots(&mut transaction).await;

    match rewritten {
        Ok(()) => transaction.commit().await.map_err(RunError::Commit),
        Err(rewrite) => match transaction.rollback().await {
            Ok(()) => Err(RunError::Rewrite(rewrite)),
            Err(rollback) => Err(RunError::RewriteAndRollback { rewrite, rollback }),
        },
    }
}

/// The migration's post-validation: every stored snapshot is in the shape
/// after the migration.
async fn post_validate(connection: &mut sqlx::PgConnection) -> Result<(), PostValidateError> {
    let mut after = None;

    loop {
        let batch = read_batch(connection, after)
            .await
            .map_err(PostValidateError::ReadBatch)?;

        let Some((last_vm_id, _snapshot)) = batch.last() else {
            return Ok(());
        };
        after = Some(*last_vm_id);

        for (vm_id, snapshot) in &batch {
            check_rewritten_snapshot(snapshot).map_err(|error| {
                PostValidateError::CheckSnapshot {
                    vm_id: *vm_id,
                    error,
                }
            })?;
        }
    }
}

impl waymark_migrations_engine_migration::PreValidateMigration for Migration {
    type Error = core::convert::Infallible;

    /// Nothing to validate ahead of the run: the run refuses the first
    /// snapshot that is not in the shape before the migration, and leaves
    /// every snapshot as it was then.
    fn pre_validate_migration<'a>(
        &'a self,
        _connection: &'a mut sqlx::PgConnection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        core::future::ready(Ok(()))
    }
}

impl waymark_migrations_engine_migration::RunMigration for Migration {
    type Error = RunError;

    fn run_migration<'a>(
        &'a self,
        connection: &'a mut sqlx::PgConnection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        run(connection)
    }
}

impl waymark_migrations_engine_migration::PostValidateMigration for Migration {
    type Error = PostValidateError;

    fn post_validate_migration<'a>(
        &'a self,
        connection: &'a mut sqlx::PgConnection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        post_validate(connection)
    }
}

#[cfg(test)]
mod tests;
