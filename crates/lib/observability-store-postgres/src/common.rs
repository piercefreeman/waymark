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
/// cannot leave a sweep making no progress. While a chunk runs it holds
/// its rows' locks, so a writer to those rows waits for the chunk.
pub(crate) const RETENTION_CHUNK: std::num::NonZeroU32 = std::num::NonZeroU32::new(10_000).unwrap();

/// Delete every row of `table` whose `column` is before `cutoff`, `chunk`
/// rows per statement, until a chunk comes up short; returns how many rows
/// went.
///
/// The sweep never waits on a row another transaction holds: the row is
/// skipped and left to a later sweep. Each chunk is one autocommit
/// statement that never waits on a row lock while holding the ones it
/// took, so it cannot close a row-lock cycle with a writer of the same
/// table, whatever order that writer locks its rows in.
///
/// `table` and `column` are the store's own identifiers, not input.
pub(crate) async fn delete_before_in_chunks(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    chunk: std::num::NonZeroU32,
) -> Result<u64, sqlx::Error> {
    // The chunk is picked in `column` order: without the ORDER BY the
    // statement's cached generic plan may scan the table for every chunk
    // (a LIMIT makes that look cheap to the planner), and the chunk that
    // ends the sweep — the one that comes up short — scans all of it.
    // Ordered, the index can serve every chunk.
    //
    // Each chunk starts where the last one ended — at the latest `column`
    // it deleted — and not at the low end of the index, where the entries
    // of the rows the earlier chunks deleted stay until VACUUM removes
    // them. The bound is inclusive: rows sharing that `column` value can
    // straddle the two chunks.
    //
    // The cutoff is repeated on the outer statement as a second check: the
    // locking clause already re-checks a row another transaction updated
    // against the inner cutoff before listing it.
    //
    // A skipped locked row does not use up the chunk: the scan reads on
    // past it, so a chunk fills unless no unlocked row is left between its
    // floor and the cutoff. A row skipped anywhere in the sweep goes to the
    // next sweep.
    let statement = format!(
        r#"
        WITH deleted AS (
            DELETE FROM {table}
            WHERE {column} < $1
              AND ctid = ANY (ARRAY(
                SELECT ctid
                FROM {table}
                WHERE {column} < $1
                  AND {column} >= COALESCE($3::timestamptz, '-infinity')
                ORDER BY {column}
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ))
            RETURNING {column}
        )
        SELECT count(*), max({column}) FROM deleted
        "#
    );

    let mut deleted = 0;
    let mut from: Option<chrono::DateTime<chrono::Utc>> = None;
    loop {
        let (rows, last): (i64, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(&statement)
            .bind(cutoff)
            .bind(i64::from(chunk.get()))
            .bind(from)
            .fetch_one(pool)
            .await?;
        let rows = u64::try_from(rows).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
        deleted += rows;
        if rows < u64::from(chunk.get()) {
            return Ok(deleted);
        }
        from = last;
    }
}
