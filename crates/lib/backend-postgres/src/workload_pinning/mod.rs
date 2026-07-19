//! Postgres backend for workload pinning.
//!
//! Implements [`waymark_workload_pinning_backend::Backend`] so that the
//! workload pinning manager can pin, refresh, and unpin workloads via
//! Postgres.

pub mod error;

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use nonempty_collections::{
    IntoIteratorExt as _, IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _,
};
use sqlx::Row;
use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_timed_future::TimedFutureExt as _;
use waymark_workload_pinning_backend::{Pinning, PinningStatus};
use waymark_workload_pinning_core::UnpinMode;

use crate::PostgresBackend;

impl waymark_workload_pinning_backend::HasTimestamp for PostgresBackend {
    type Timestamp = DateTime<Utc>;
}

impl waymark_workload_pinning_backend::HasNodeId for PostgresBackend {
    type NodeId = uuid::Uuid;
}

impl waymark_workload_pinning_backend::HasWorkloadId for PostgresBackend {
    type WorkloadId = InstanceId;
}

impl waymark_workload_pinning_backend::PollUnpinnedWorkloads for PostgresBackend {
    type Error = error::PollError;

    #[obs]
    #[function_name::named]
    async fn poll_unpinned(
        &self,
        now: Self::Timestamp,
        pinning: Pinning<Self::NodeId, Self::Timestamp>,
        max_items: NonZeroUsize,
    ) -> Result<Option<NEVec<Self::WorkloadId>>, Self::Error> {
        Self::count_query(&self.query_counts, "update:runnable_workloads_poll");
        // Staleness check and fresh expiry both use the database clock:
        // the steal comparison is single-clock and needs no cross-node
        // time agreement.
        let rows = sqlx::query(
            r#"
            UPDATE runnable_workloads
            SET node_id = $2, expires_at = NOW() + ($3 * interval '1 microsecond'), updated_at = NOW()
            WHERE workload_id IN (
                SELECT workload_id
                FROM runnable_workloads
                WHERE node_id IS NULL OR expires_at <= NOW()
                ORDER BY updated_at
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING workload_id
            "#,
        )
        .bind(max_items.get() as i64)
        .bind(pinning.node_id)
        .bind(crate::remaining_micros(now, pinning.expires_at))
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!(
            "update:runnable_workloads_poll"
        ))
        .await
        .map_err(error::PollError::Sqlx)?;

        let Some(rows) = rows.try_into_nonempty_iter() else {
            return Ok(None);
        };

        Ok(Some(rows.map(|row| row.get("workload_id")).collect()))
    }
}

impl waymark_workload_pinning_backend::KeepalivePinnings for PostgresBackend {
    type Error = error::RefreshError;

    #[obs]
    #[function_name::named]
    fn refresh_pinnings<'a>(
        &'a self,
        now: Self::Timestamp,
        pinning: Pinning<Self::NodeId, Self::Timestamp>,
        workload_ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Self::WorkloadId> + 'a,
    ) -> impl Future<
        Output = Result<
            NEVec<PinningStatus<Self::WorkloadId, Pinning<Self::NodeId, Self::Timestamp>>>,
            Self::Error,
        >,
    > + Send
    + 'a {
        let ids: NEVec<InstanceId> = workload_ids.into_nonempty_iter().collect();
        async move {
            Self::count_query(&self.query_counts, "update:runnable_workloads_refresh");
            // Re-fence every row still owned by this node, even if it has
            // already lapsed past `expires_at`. Ownership (`node_id`) is the
            // source of truth: a steal by another node rewrites `node_id` and a
            // release nulls it, so either makes this UPDATE match zero rows and
            // report the pinning as lost. Row-level locking serializes a
            // concurrent poll-steal against this refresh, so whichever commits
            // first wins deterministically. Guarding on `expires_at > NOW()` here
            // would instead drop a still-owned pinning that nobody contested
            // just because a heartbeat landed late.
            let rows = sqlx::query(
                r#"
                UPDATE runnable_workloads
                SET expires_at = NOW() + ($3 * interval '1 microsecond'), updated_at = NOW()
                WHERE node_id = $1 AND workload_id = ANY($2)
                RETURNING workload_id
                "#,
            )
            .bind(pinning.node_id)
            .bind(ids.as_ref())
            .bind(crate::remaining_micros(now, pinning.expires_at))
            .fetch_all(&self.pool)
            .timed(crate::query_timing_histogram!(
                "update:runnable_workloads_refresh"
            ))
            .await
            .map_err(error::RefreshError::Sqlx)?;

            let refreshed: std::collections::HashSet<InstanceId> =
                rows.iter().map(|row| row.get("workload_id")).collect();

            let statuses = ids
                .into_nonempty_iter()
                .map(|id| PinningStatus {
                    workload_id: id,
                    pinning: if refreshed.contains(&id) {
                        Some(Pinning {
                            node_id: pinning.node_id,
                            expires_at: pinning.expires_at,
                        })
                    } else {
                        None
                    },
                })
                .collect();

            Ok(statuses)
        }
    }
}

impl waymark_workload_pinning_backend::UnpinWorkloads for PostgresBackend {
    type Error = error::UnpinError;

    #[obs]
    #[function_name::named]
    fn unpin_workloads<'a>(
        &'a self,
        node_id: Self::NodeId,
        workloads: impl nonempty_collections::IntoNonEmptyIterator<Item = (Self::WorkloadId, UnpinMode)>
        + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        let mut released: Vec<InstanceId> = Vec::new();
        let mut parked: Vec<InstanceId> = Vec::new();
        for (workload_id, mode) in workloads {
            match mode {
                UnpinMode::Release => released.push(workload_id),
                UnpinMode::Park => parked.push(workload_id),
            }
        }
        async move {
            Self::count_query(&self.query_counts, "update:runnable_workloads_unpin");
            sqlx::query(
                r#"
                WITH parked AS (
                    DELETE FROM runnable_workloads
                    WHERE node_id = $1 AND workload_id = ANY($2)
                )
                UPDATE runnable_workloads
                SET node_id = NULL, expires_at = NULL, updated_at = NOW()
                WHERE node_id = $1 AND workload_id = ANY($3)
                "#,
            )
            .bind(node_id)
            .bind(&parked)
            .bind(&released)
            .execute(&self.pool)
            .timed(crate::query_timing_histogram!(
                "update:runnable_workloads_unpin"
            ))
            .await
            .map_err(error::UnpinError::Sqlx)?;

            Ok(())
        }
    }
}
