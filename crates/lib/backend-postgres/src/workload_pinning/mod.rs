//! Postgres backend for workload pinning.
//!
//! Implements [`waymark_workload_pinning_backend::Backend`] so that the
//! workload pinning manager can pin, refresh, and release workload
//! pinnings via Postgres.

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
        let rows = sqlx::query(
            r#"
            UPDATE runnable_workloads
            SET node_id = $3, expires_at = $4, updated_at = NOW()
            WHERE workload_id IN (
                SELECT workload_id
                FROM runnable_workloads
                WHERE node_id IS NULL OR expires_at <= $1
                ORDER BY updated_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING workload_id
            "#,
        )
        .bind(now)
        .bind(max_items.get() as i64)
        .bind(pinning.node_id)
        .bind(pinning.expires_at)
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
        // TODO: the trait should be updated to drop this param
        _now: Self::Timestamp,
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
            // first wins deterministically. Guarding on `expires_at > now` here
            // would instead drop a still-owned pinning that nobody contested
            // just because a heartbeat landed late.
            let rows = sqlx::query(
                r#"
                UPDATE runnable_workloads
                SET expires_at = $3, updated_at = NOW()
                WHERE node_id = $1 AND workload_id = ANY($2)
                RETURNING workload_id
                "#,
            )
            .bind(pinning.node_id)
            .bind(ids.as_ref())
            .bind(pinning.expires_at)
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

impl waymark_workload_pinning_backend::ReleasePinnings for PostgresBackend {
    type Error = error::ReleaseError;

    #[obs]
    #[function_name::named]
    fn release_pinnings<'a>(
        &'a self,
        node_id: Self::NodeId,
        workload_ids: impl nonempty_collections::IntoNonEmptyIterator<Item = Self::WorkloadId> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        let ids: Vec<InstanceId> = workload_ids.into_iter().collect();
        async move {
            Self::count_query(&self.query_counts, "update:runnable_workloads_release");
            sqlx::query(
                r#"
                UPDATE runnable_workloads
                SET node_id = NULL, expires_at = NULL, updated_at = NOW()
                WHERE node_id = $1 AND workload_id = ANY($2)
                "#,
            )
            .bind(node_id)
            .bind(&ids)
            .execute(&self.pool)
            .timed(crate::query_timing_histogram!(
                "update:runnable_workloads_release"
            ))
            .await
            .map_err(error::ReleaseError::Sqlx)?;

            Ok(())
        }
    }
}
