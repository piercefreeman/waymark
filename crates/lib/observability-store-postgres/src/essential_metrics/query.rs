//! The query side: reading samples back.

use sqlx::Row as _;
use waymark_essential_metrics_core::NodeSample;

use super::common::{NODE_SAMPLE_COLUMNS, elementwise_sum};
use crate::Store;

/// A stored value outside the domain of its column: the column is a
/// `bigint`, the value is unsigned, and a negative value is corruption
/// — never clamped, always surfaced.
///
/// Surfaces as [`sqlx::Error::Decode`], downcastable to this type.
#[derive(Debug, thiserror::Error)]
#[error("{column}: negative value {value}")]
pub struct NegativeColumnValueError {
    /// The column read.
    pub column: String,

    /// The stored value.
    pub value: i64,
}

/// A stored histogram with the wrong number of buckets for its metric:
/// the bounds are fixed at compile time, so a row of another width is
/// corruption — never padded or truncated, always surfaced.
///
/// Surfaces as [`sqlx::Error::Decode`], downcastable to this type.
#[derive(Debug, thiserror::Error)]
#[error("{column}: expected {expected} buckets, got {actual}")]
pub struct HistogramBucketCountError {
    /// The column read.
    pub column: String,

    /// The bucket count the metric is defined with.
    pub expected: usize,

    /// The bucket count the row holds.
    pub actual: usize,
}

/// Bring a stored `bigint` back into the unsigned domain it was written
/// from.
fn to_unsigned(column: &str, value: i64) -> Result<u64, sqlx::Error> {
    let unsigned = u64::try_from(value).map_err(|_| {
        sqlx::Error::Decode(Box::new(NegativeColumnValueError {
            column: column.to_owned(),
            value,
        }))
    })?;
    Ok(unsigned)
}

/// Read a fixed-length histogram out of a `bigint[]` column and its
/// paired sum.
fn decode_histogram<const N: usize>(
    row: &sqlx::postgres::PgRow,
    metric: &str,
) -> Result<waymark_essential_metrics_core::BucketedHistogram<N>, sqlx::Error> {
    let counts_column = format!("{metric}_counts");
    let counts: Vec<i64> = row.try_get(counts_column.as_str())?;
    let actual = counts.len();
    let counts: [i64; N] = counts.try_into().map_err(|_| {
        sqlx::Error::Decode(Box::new(HistogramBucketCountError {
            column: counts_column.clone(),
            expected: N,
            actual,
        }))
    })?;
    let mut unsigned_counts = [0; N];
    for (slot, count) in unsigned_counts.iter_mut().zip(counts) {
        *slot = to_unsigned(&counts_column, count)?;
    }
    let sum_column = format!("{metric}_sum");
    let sum = row.try_get(sum_column.as_str())?;
    Ok(waymark_essential_metrics_core::BucketedHistogram {
        counts: unsigned_counts,
        sum,
    })
}

/// Read one sample from a row shaped like [`NODE_SAMPLE_COLUMNS`].
fn decode_sample(
    row: &sqlx::postgres::PgRow,
) -> Result<NodeSample<waymark_ids::NodeId>, sqlx::Error> {
    let node_id: waymark_ids::NodeId = row.try_get("node_id")?;
    let as_u64 = |column: &str| -> Result<u64, sqlx::Error> {
        let value: i64 = row.try_get(column)?;
        to_unsigned(column, value)
    };
    Ok(NodeSample {
        node_id,
        sampled_at: row.try_get("sampled_at")?,
        worker_pool_size: as_u64("worker_pool_size")?,
        max_in_flight_actions: as_u64("max_in_flight_actions")?,
        in_flight_actions: as_u64("in_flight_actions")?,
        queued_action_dispatches: as_u64("queued_action_dispatches")?,
        driven_vm_runtimes: as_u64("driven_vm_runtimes")?,
        actions_completed_total: as_u64("actions_completed_total")?,
        last_action_completed_at: row.try_get("last_action_completed_at")?,
        action_dequeue_seconds: decode_histogram(row, "action_dequeue_seconds")?,
        action_handling_seconds: decode_histogram(row, "action_handling_seconds")?,
        essential_metrics_dropped_total: as_u64("essential_metrics_dropped_total")?,
    })
}

impl waymark_essential_metrics_query_backend::HasNodeId for Store {
    type NodeId = waymark_ids::NodeId;
}

impl waymark_essential_metrics_query_backend::Latest for Store {
    type Error = sqlx::Error;

    async fn latest(&self) -> Result<Vec<NodeSample<waymark_ids::NodeId>>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            r#"
            SELECT DISTINCT ON (node_id) {NODE_SAMPLE_COLUMNS}
            FROM essential_metrics_node_samples
            ORDER BY node_id, sampled_at DESC
            "#,
        ))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_sample).collect()
    }
}

impl waymark_essential_metrics_query_backend::Series for Store {
    type Error = sqlx::Error;

    async fn series(
        &self,
        params: waymark_essential_metrics_query_backend::series::Params<waymark_ids::NodeId>,
    ) -> Result<Vec<NodeSample<waymark_ids::NodeId>>, sqlx::Error> {
        // Counts and their sums add across the rows of a bucket, which is
        // exactly what makes them roll up: a quantile in their place
        // could only be averaged, which means nothing.
        let dequeue_counts = elementwise_sum(
            "action_dequeue_seconds_counts",
            waymark_essential_metrics_core::ACTION_DEQUEUE_SECONDS_BOUNDS.len() + 1,
        );
        let handling_counts = elementwise_sum(
            "action_handling_seconds_counts",
            waymark_essential_metrics_core::ACTION_HANDLING_SECONDS_BOUNDS.len() + 1,
        );
        let rows = sqlx::query(&format!(
            r#"
            SELECT
                node_id,
                date_bin(make_interval(secs => $4), sampled_at, $2) AS sampled_at,
                avg(worker_pool_size)::bigint AS worker_pool_size,
                avg(max_in_flight_actions)::bigint AS max_in_flight_actions,
                avg(in_flight_actions)::bigint AS in_flight_actions,
                avg(queued_action_dispatches)::bigint AS queued_action_dispatches,
                avg(driven_vm_runtimes)::bigint AS driven_vm_runtimes,
                max(actions_completed_total) AS actions_completed_total,
                max(last_action_completed_at) AS last_action_completed_at,
                {dequeue_counts} AS action_dequeue_seconds_counts,
                sum(action_dequeue_seconds_sum) AS action_dequeue_seconds_sum,
                {handling_counts} AS action_handling_seconds_counts,
                sum(action_handling_seconds_sum) AS action_handling_seconds_sum,
                max(essential_metrics_dropped_total) AS essential_metrics_dropped_total
            FROM essential_metrics_node_samples
            WHERE node_id = $1 AND sampled_at >= $2 AND sampled_at < $3
            GROUP BY node_id, 2
            ORDER BY 2
            "#,
        ))
        .bind(params.node_id)
        .bind(params.from)
        .bind(params.to)
        .bind(params.bucket.get().as_secs_f64())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_sample).collect()
    }
}
