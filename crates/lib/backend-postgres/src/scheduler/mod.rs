//! Postgres backend for the scheduler's firing side.
//!
//! Implements the `waymark_scheduler_backend` traits for
//! [`crate::PostgresBackend`].

pub mod error;

#[cfg(test)]
mod tests;

use nonempty_collections::{IntoIteratorExt as _, NonEmptyIterator as _};
use sqlx::Row as _;
use waymark_ids::InstanceId;
use waymark_observability::obs;
use waymark_scheduler_backend::poll_due_schedules::DueSchedule;
use waymark_scheduler_backend::register_scheduled_vm_runtimes::{Item, Outcome};
use waymark_timed_future::TimedFutureExt as _;

use crate::PostgresBackend;

impl waymark_scheduler_backend::HasVmId for PostgresBackend {
    type VmId = InstanceId;
}

impl waymark_scheduler_backend::HasTimestamp for PostgresBackend {
    type Timestamp = chrono::DateTime<chrono::Utc>;
}

impl waymark_scheduler_backend::PollDueSchedules for PostgresBackend {
    type Error = error::PollDueSchedulesError;

    #[obs]
    #[function_name::named]
    async fn poll_due_schedules(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        max_items: std::num::NonZeroUsize,
    ) -> Result<
        Option<nonempty_collections::NEVec<DueSchedule<InstanceId, chrono::DateTime<chrono::Utc>>>>,
        Self::Error,
    > {
        Self::count_query(&self.query_counts, "select:schedules_due");
        let rows = sqlx::query(
            r#"
            SELECT schedule_name, definition, next_run_at, last_instance_id
            FROM schedules
            WHERE status = 'active' AND next_run_at <= $1
            ORDER BY next_run_at
            LIMIT $2
            "#,
        )
        .bind(now)
        .bind(max_items.get() as i64)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!("select:schedules_due"))
        .await
        .map_err(error::PollDueSchedulesError::Sqlx)?;

        let Some(rows) = rows.try_into_nonempty_iter() else {
            return Ok(None);
        };

        Ok(Some(
            rows.map(|row| DueSchedule {
                schedule_name: row.get("schedule_name"),
                definition: row.get("definition"),
                next_run_at: row.get("next_run_at"),
                last_instance_id: row.get("last_instance_id"),
            })
            .collect(),
        ))
    }
}

impl waymark_scheduler_backend::RegisterScheduledVmRuntimes for PostgresBackend {
    type Error = error::RegisterScheduledVmRuntimesError;

    #[obs]
    #[function_name::named]
    async fn register_scheduled_vm_runtimes<'a>(
        &'a self,
        items: nonempty_collections::NESlice<
            'a,
            Item<'a, InstanceId, chrono::DateTime<chrono::Utc>>,
        >,
    ) -> Result<nonempty_collections::NEVec<Outcome>, Self::Error> {
        Self::count_query(&self.query_counts, "update:schedules_spawn");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:schedules_spawn",
            items.len().get(),
        );

        let mut schedule_names: Vec<&str> = Vec::with_capacity(items.len().get());
        let mut expected_next_run_ats = Vec::with_capacity(items.len().get());
        let mut vm_ids = Vec::with_capacity(items.len().get());
        let mut new_next_run_ats = Vec::with_capacity(items.len().get());
        let mut check_overlaps = Vec::with_capacity(items.len().get());
        for item in items.iter() {
            schedule_names.push(&*item.schedule_name);
            expected_next_run_ats.push(*item.expected_next_run_at);
            vm_ids.push(*item.vm_id);
            new_next_run_ats.push(*item.new_next_run_at);
            check_overlaps.push(item.check_overlap);
        }

        // One statement, one implicit transaction, per-row outcomes:
        //
        //  - `gated` snapshots each input row's schedule where the fence
        //    matches, computing whether the overlap gate blocks it (the
        //    last spawned instance still has its snapshot row and no
        //    execution result).
        //  - `advanced` applies the cursor advance. The fence predicate is
        //    repeated HERE, in the UPDATE's own WHERE: under READ
        //    COMMITTED a concurrent registrar's committed advance makes
        //    the re-evaluated predicate fail, so exactly one competitor
        //    wins each occurrence. A blocked row advances its cursor but
        //    keeps its last-instance marker.
        //  - The two inserts register the VM runtime (snapshot + runnable
        //    workload) for the unblocked winners, copying the schedule's
        //    baked initial snapshot under the fresh vm id.
        //  - The final projection reports per input position: no
        //    `advanced` row = superseded, blocked = skipped, else
        //    registered.
        let rows = sqlx::query(
            r#"
            WITH input AS (
                SELECT *
                FROM UNNEST($1::text[], $2::timestamptz[], $3::uuid[], $4::timestamptz[], $5::boolean[])
                    WITH ORDINALITY
                    AS t(schedule_name, expected_next_run_at, vm_id, new_next_run_at, check_overlap, input_position)
            ),
            gated AS (
                SELECT input.input_position,
                       input.schedule_name,
                       input.expected_next_run_at,
                       input.vm_id,
                       input.new_next_run_at,
                       schedules.executable_id,
                       schedules.initial_snapshot,
                       (
                           input.check_overlap
                           AND schedules.last_instance_id IS NOT NULL
                           AND EXISTS (
                               SELECT 1 FROM vm_runtime_snapshots
                               WHERE vm_runtime_snapshots.vm_id = schedules.last_instance_id
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM vm_execution_results
                               WHERE vm_execution_results.vm_id = schedules.last_instance_id
                           )
                       ) AS blocked
                FROM schedules
                JOIN input ON schedules.schedule_name = input.schedule_name
                WHERE schedules.status = 'active'
                  AND schedules.next_run_at = input.expected_next_run_at
            ),
            advanced AS (
                UPDATE schedules
                SET next_run_at = gated.new_next_run_at,
                    last_instance_id = CASE
                        WHEN gated.blocked THEN schedules.last_instance_id
                        ELSE gated.vm_id
                    END
                FROM gated
                WHERE schedules.schedule_name = gated.schedule_name
                  AND schedules.status = 'active'
                  AND schedules.next_run_at = gated.expected_next_run_at
                RETURNING gated.input_position,
                          gated.vm_id,
                          gated.executable_id,
                          gated.initial_snapshot,
                          gated.blocked
            ),
            inserted_snapshots AS (
                INSERT INTO vm_runtime_snapshots (vm_id, executable_id, snapshot)
                SELECT vm_id, executable_id, initial_snapshot
                FROM advanced
                WHERE NOT blocked
                RETURNING vm_id
            ),
            inserted_workloads AS (
                INSERT INTO runnable_workloads (workload_id)
                SELECT vm_id FROM inserted_snapshots
                RETURNING workload_id
            )
            SELECT input.input_position,
                   advanced.input_position IS NOT NULL AS matched,
                   COALESCE(advanced.blocked, FALSE) AS blocked
            FROM input
            LEFT JOIN advanced ON advanced.input_position = input.input_position
            ORDER BY input.input_position
            "#,
        )
        .bind(&schedule_names)
        .bind(&expected_next_run_ats)
        .bind(&vm_ids)
        .bind(&new_next_run_ats)
        .bind(&check_overlaps)
        .fetch_all(&self.pool)
        .timed(crate::query_timing_histogram!("update:schedules_spawn"))
        .await
        .map_err(error::RegisterScheduledVmRuntimesError::Sqlx)?;

        let outcomes: Vec<Outcome> = rows
            .iter()
            .map(|row| {
                let matched: bool = row.get("matched");
                let blocked: bool = row.get("blocked");
                if !matched {
                    Outcome::Superseded
                } else if blocked {
                    Outcome::SkippedOverlap
                } else {
                    Outcome::Registered
                }
            })
            .collect();
        Ok(nonempty_collections::NEVec::try_from_vec(outcomes)
            .expect("the projection returns one row per input item, and the input is non-empty"))
    }
}
