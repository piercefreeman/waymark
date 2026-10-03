//! Shared by every observability subsystem's modules.

pub mod delete_before_in_chunks;

pub(crate) use self::delete_before_in_chunks::delete_before_in_chunks;

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
