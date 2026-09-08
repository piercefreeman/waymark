//! Shared by every observability subsystem's modules.

/// Saturate an unsigned value into the `bigint` column domain.
pub fn to_bigint_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// How many rows one statement of a retention sweep deletes.
///
/// A sweep deletes in chunks, one statement each, so no statement runs
/// long however far behind the sweep is: the sinks interleave between
/// chunks, and a chunk is bounded work, so the pool's statement timeout
/// cannot leave a sweep making no progress.
pub(crate) const RETENTION_CHUNK: u32 = 10_000;

/// Delete every row of `table` whose `column` is before `cutoff`, `chunk`
/// rows per statement, until a chunk comes up short; returns how many rows
/// went.
///
/// The sweep never waits on a row another transaction holds: the row is
/// skipped and left to a later sweep. A statement that never waits cannot
/// be part of a lock cycle, so the sweep cannot deadlock a writer of the
/// same table — the absorb trigger upserts `observability_vm_instances`
/// in a different row order than the sweep deletes in.
///
/// `table` and `column` are the store's own identifiers, not input.
pub(crate) async fn delete_before_in_chunks(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    chunk: u32,
) -> Result<u64, sqlx::Error> {
    // The chunk is picked in `column` order: without the ORDER BY the
    // statement's cached generic plan may scan the table for every chunk
    // (a LIMIT makes that look cheap to the planner), and the chunk that
    // ends the sweep — the one that comes up short — scans all of it.
    // Ordered, the index can serve every chunk.
    //
    // The cutoff is repeated on the outer statement so a row that another
    // transaction updates while it is listed is re-checked against it
    // rather than deleted in its new version.
    //
    // A chunk that skipped locked rows comes up short and ends the sweep
    // with those rows left; the next sweep takes them.
    let statement = format!(
        r#"
        DELETE FROM {table}
        WHERE {column} < $1
          AND ctid = ANY (ARRAY(
            SELECT ctid
            FROM {table}
            WHERE {column} < $1
            ORDER BY {column}
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        ))
        "#
    );

    let mut deleted = 0;
    loop {
        let result = sqlx::query(&statement)
            .bind(cutoff)
            .bind(i64::from(chunk))
            .execute(pool)
            .await?;
        let rows = result.rows_affected();
        deleted += rows;
        if rows < u64::from(chunk) {
            return Ok(deleted);
        }
    }
}
