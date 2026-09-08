//! Shared by every observability subsystem's modules.

/// Saturate an unsigned value into the `bigint` column domain.
pub fn to_bigint_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// How many rows one statement of a retention sweep deletes.
///
/// A sweep deletes in chunks, one statement each, so no statement runs
/// long however far behind the sweep is: the sinks interleave between
/// chunks, and the pool's statement timeout never cuts a sweep short.
pub(crate) const RETENTION_CHUNK: u32 = 10_000;

/// Delete every row of `table` whose `column` is before `cutoff`, `chunk`
/// rows per statement, until none is left; returns how many rows went.
///
/// `table` and `column` are the store's own identifiers, not input.
pub(crate) async fn delete_before_in_chunks(
    pool: &sqlx::PgPool,
    table: &str,
    column: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    chunk: u32,
) -> Result<u64, sqlx::Error> {
    let statement = format!(
        r#"
        DELETE FROM {table}
        WHERE ctid = ANY (ARRAY(
            SELECT ctid
            FROM {table}
            WHERE {column} < $1
            LIMIT $2
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
