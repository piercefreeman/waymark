//! Vocabulary shared by every family's modules.

/// Saturate an unsigned value into the `bigint` column domain.
pub fn to_bigint_saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
