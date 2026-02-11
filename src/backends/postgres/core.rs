use std::collections::HashMap;
use std::future::Future;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use sqlx::{Postgres, QueryBuilder, Row};
use tonic::async_trait;
use tracing::warn;
use uuid::Uuid;

use super::PostgresBackend;
use crate::backends::base::{
    ActionDone, BackendError, BackendResult, CoreBackend, GarbageCollectionResult,
    GarbageCollectorBackend, GraphUpdate, InstanceDone, InstanceLockStatus, LockClaim,
    QueuedInstance, QueuedInstanceBatch, WorkerStatusBackend, WorkerStatusUpdate,
};
use crate::observability::obs;
use crate::waymark_core::runner::state::RunnerState;

const INSTANCE_STATUS_QUEUED: &str = "queued";
const INSTANCE_STATUS_RUNNING: &str = "running";
const INSTANCE_STATUS_COMPLETED: &str = "completed";
const INSTANCE_STATUS_FAILED: &str = "failed";
const TRANSIENT_DEADLOCK_SQLSTATE: &str = "40P01";
const TRANSIENT_SERIALIZATION_SQLSTATE: &str = "40001";
const TRANSIENT_RETRY_MAX_ATTEMPTS: usize = 3;
const TRANSIENT_RETRY_INITIAL_BACKOFF_MS: u64 = 25;
const TRANSIENT_RETRY_MAX_BACKOFF_MS: u64 = 250;

fn instance_result_is_error_wrapper(result: &serde_json::Value) -> bool {
    let serde_json::Value::Object(map) = result else {
        return false;
    };
    map.len() == 1
        && (map.contains_key("error")
            || map.contains_key("__exception__")
            || map.contains_key("exception"))
}

fn instance_done_status(instance: &InstanceDone) -> &'static str {
    if instance.error.is_some()
        || instance
            .result
            .as_ref()
            .is_some_and(instance_result_is_error_wrapper)
    {
        INSTANCE_STATUS_FAILED
    } else {
        INSTANCE_STATUS_COMPLETED
    }
}

fn is_transient_sqlstate(code: &str) -> bool {
    matches!(
        code,
        TRANSIENT_DEADLOCK_SQLSTATE | TRANSIENT_SERIALIZATION_SQLSTATE
    )
}

fn is_transient_backend_error(err: &BackendError) -> bool {
    match err {
        BackendError::Sqlx(sqlx::Error::Database(db_err)) => {
            db_err.code().as_deref().is_some_and(is_transient_sqlstate)
        }
        // Fallback for cases where sqlstate is not preserved in wrapping.
        BackendError::Message(message) => {
            message.contains("deadlock detected")
                || message.contains("could not serialize access due to")
        }
        _ => false,
    }
}

async fn retry_transient_backend<T, Op, Fut>(
    operation: &'static str,
    mut op: Op,
) -> BackendResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = BackendResult<T>>,
{
    let mut attempt = 0usize;
    let mut backoff_ms = TRANSIENT_RETRY_INITIAL_BACKOFF_MS;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err)
                if attempt < TRANSIENT_RETRY_MAX_ATTEMPTS && is_transient_backend_error(&err) =>
            {
                attempt += 1;
                warn!(
                    operation,
                    attempt,
                    error = %err,
                    "transient database error; retrying"
                );
                tokio::time::sleep(StdDuration::from_millis(backoff_ms)).await;
                backoff_ms =
                    std::cmp::min(backoff_ms.saturating_mul(2), TRANSIENT_RETRY_MAX_BACKOFF_MS);
            }
            Err(err) => return Err(err),
        }
    }
}

impl PostgresBackend {
    /// Insert queued instances for run-loop consumption.
    #[obs]
    pub async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        if instances.is_empty() {
            return Ok(());
        }
        let workflow_version_ids: Vec<Uuid> = instances
            .iter()
            .map(|instance| instance.workflow_version_id)
            .collect();
        let workflow_rows =
            sqlx::query("SELECT id, workflow_name FROM workflow_versions WHERE id = ANY($1)")
                .bind(&workflow_version_ids)
                .fetch_all(&self.pool)
                .await?;
        let mut workflow_names_by_version_id: HashMap<Uuid, String> =
            HashMap::with_capacity(workflow_rows.len());
        for row in workflow_rows {
            workflow_names_by_version_id.insert(row.get("id"), row.get("workflow_name"));
        }

        let mut queued_payloads = Vec::new();
        let mut runner_payloads = Vec::new();
        for instance in instances {
            let state = instance.state.as_ref().ok_or_else(|| {
                BackendError::Message("queued instance missing runner state".to_string())
            })?;
            let scheduled_at = instance.scheduled_at.unwrap_or_else(Utc::now);
            let workflow_name = workflow_names_by_version_id
                .get(&instance.workflow_version_id)
                .cloned();
            let mut payload_instance = instance.clone();
            payload_instance.scheduled_at = Some(scheduled_at);
            queued_payloads.push((
                payload_instance.instance_id,
                scheduled_at,
                workflow_name.clone(),
                INSTANCE_STATUS_QUEUED,
                Self::serialize(&payload_instance)?,
            ));
            let graph = GraphUpdate::from_state(instance.instance_id, state);
            runner_payloads.push((
                instance.instance_id,
                instance.entry_node,
                instance.workflow_version_id,
                instance.schedule_id,
                workflow_name,
                INSTANCE_STATUS_QUEUED,
                Self::serialize(&graph)?,
            ));
        }

        let mut queued_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO queued_instances (instance_id, scheduled_at, workflow_name, current_status, payload) ",
        );
        queued_builder.push_values(
            queued_payloads.iter(),
            |mut builder, (id, scheduled_at, workflow_name, current_status, payload)| {
                builder
                    .push_bind(*id)
                    .push_bind(*scheduled_at)
                    .push_bind(workflow_name.as_deref())
                    .push_bind(*current_status)
                    .push_bind(payload.as_slice());
            },
        );

        let mut runner_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO runner_instances (instance_id, entry_node, workflow_version_id, schedule_id, workflow_name, current_status, state) ",
        );
        runner_builder.push_values(
            runner_payloads.iter(),
            |mut builder,
             (
                id,
                entry,
                workflow_version_id,
                schedule_id,
                workflow_name,
                current_status,
                payload,
            )| {
                builder
                    .push_bind(*id)
                    .push_bind(*entry)
                    .push_bind(*workflow_version_id)
                    .push_bind(*schedule_id)
                    .push_bind(workflow_name.as_deref())
                    .push_bind(*current_status)
                    .push_bind(payload.as_slice());
            },
        );

        let mut tx = self.pool.begin().await?;
        Self::count_query(&self.query_counts, "insert:queued_instances");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:queued_instances",
            instances.len(),
        );
        queued_builder.build().execute(&mut *tx).await?;
        Self::count_query(&self.query_counts, "insert:runner_instances");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:runner_instances",
            instances.len(),
        );
        runner_builder.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Upsert worker status for monitoring and activity graphs.
    #[obs]
    pub async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "upsert:worker_status");
        sqlx::query(
            r#"
            INSERT INTO worker_status (
                pool_id,
                worker_id,
                throughput_per_min,
                total_completed,
                last_action_at,
                updated_at,
                median_dequeue_ms,
                median_handling_ms,
                dispatch_queue_size,
                total_in_flight,
                active_workers,
                actions_per_sec,
                median_instance_duration_secs,
                active_instance_count,
                total_instances_completed,
                instances_per_sec,
                instances_per_min,
                time_series
            )
            VALUES ($1, 0, $2, $3, $4, NOW(), $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (pool_id, worker_id)
            DO UPDATE SET
                throughput_per_min = EXCLUDED.throughput_per_min,
                total_completed = EXCLUDED.total_completed,
                last_action_at = EXCLUDED.last_action_at,
                updated_at = EXCLUDED.updated_at,
                median_dequeue_ms = EXCLUDED.median_dequeue_ms,
                median_handling_ms = EXCLUDED.median_handling_ms,
                dispatch_queue_size = EXCLUDED.dispatch_queue_size,
                total_in_flight = EXCLUDED.total_in_flight,
                active_workers = EXCLUDED.active_workers,
                actions_per_sec = EXCLUDED.actions_per_sec,
                median_instance_duration_secs = EXCLUDED.median_instance_duration_secs,
                active_instance_count = EXCLUDED.active_instance_count,
                total_instances_completed = EXCLUDED.total_instances_completed,
                instances_per_sec = EXCLUDED.instances_per_sec,
                instances_per_min = EXCLUDED.instances_per_min,
                time_series = EXCLUDED.time_series
            "#,
        )
        .bind(status.pool_id)
        .bind(status.throughput_per_min)
        .bind(status.total_completed)
        .bind(status.last_action_at)
        .bind(status.median_dequeue_ms)
        .bind(status.median_handling_ms)
        .bind(status.dispatch_queue_size)
        .bind(status.total_in_flight)
        .bind(status.active_workers)
        .bind(status.actions_per_sec)
        .bind(status.median_instance_duration_secs)
        .bind(status.active_instance_count)
        .bind(status.total_instances_completed)
        .bind(status.instances_per_sec)
        .bind(status.instances_per_min)
        .bind(&status.time_series)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Clear expired queue locks so they can be claimed again by the runloop.
    ///
    /// This uses the same `FOR UPDATE SKIP LOCKED` claim pattern as dequeue to
    /// avoid blocking under concurrent sweepers.
    #[obs]
    pub async fn reclaim_expired_instance_locks(&self, size: usize) -> BackendResult<usize> {
        if size == 0 {
            return Ok(0);
        }

        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        Self::count_query(&self.query_counts, "update:queued_instances_expired_unlock");
        let rows = sqlx::query(
            r#"
            WITH expired AS (
                SELECT instance_id
                FROM queued_instances
                WHERE lock_uuid IS NOT NULL
                  AND lock_expires_at <= $1
                ORDER BY lock_expires_at, scheduled_at, created_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queued_instances AS qi
            SET lock_uuid = NULL,
                lock_expires_at = NULL
            FROM expired
            WHERE qi.instance_id = expired.instance_id
            RETURNING qi.instance_id
            "#,
        )
        .bind(now)
        .bind(size as i64)
        .fetch_all(&mut *tx)
        .await?;

        if !rows.is_empty() {
            let instance_ids: Vec<Uuid> = rows.iter().map(|row| row.get("instance_id")).collect();
            sqlx::query(
                "UPDATE runner_instances SET current_status = $2 WHERE instance_id = ANY($1) AND result IS NULL AND error IS NULL",
            )
            .bind(&instance_ids)
            .bind(INSTANCE_STATUS_QUEUED)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        if !rows.is_empty() {
            Self::count_batch_size(
                &self.batch_size_counts,
                "update:queued_instances_expired_unlock",
                rows.len(),
            );
        }

        Ok(rows.len())
    }

    /// Delete old finished instances and their action attempt rows.
    #[obs]
    pub async fn collect_done_instances_impl(
        &self,
        older_than: DateTime<Utc>,
        limit: usize,
    ) -> BackendResult<GarbageCollectionResult> {
        if limit == 0 {
            return Ok(GarbageCollectionResult::default());
        }

        let mut tx = self.pool.begin().await?;
        Self::count_query(&self.query_counts, "select:runner_instances_gc_candidates");
        let candidate_rows = sqlx::query(
            r#"
            SELECT instance_id, state
            FROM runner_instances
            WHERE created_at < $1
              AND (result IS NOT NULL OR error IS NOT NULL)
            ORDER BY created_at, instance_id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(older_than)
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await?;

        if candidate_rows.is_empty() {
            tx.commit().await?;
            return Ok(GarbageCollectionResult::default());
        }

        let mut instance_ids = Vec::with_capacity(candidate_rows.len());
        let mut action_execution_ids = Vec::new();
        for row in candidate_rows {
            let instance_id: Uuid = row.get("instance_id");
            let state_payload: Option<Vec<u8>> = row.get("state");
            instance_ids.push(instance_id);

            let Some(state_payload) = state_payload else {
                continue;
            };
            match Self::deserialize::<GraphUpdate>(&state_payload) {
                Ok(graph) => {
                    for (execution_id, node) in graph.nodes {
                        if node.is_action_call() {
                            action_execution_ids.push(execution_id);
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        %instance_id,
                        error = %err,
                        "failed to decode runner state while collecting garbage"
                    );
                }
            }
        }

        action_execution_ids.sort_unstable();
        action_execution_ids.dedup();
        let deleted_actions = if action_execution_ids.is_empty() {
            0
        } else {
            Self::count_query(&self.query_counts, "delete:runner_actions_done_gc");
            let result =
                sqlx::query("DELETE FROM runner_actions_done WHERE execution_id = ANY($1)")
                    .bind(&action_execution_ids)
                    .execute(&mut *tx)
                    .await?;
            let rows = result.rows_affected() as usize;
            Self::count_batch_size(
                &self.batch_size_counts,
                "delete:runner_actions_done_gc",
                rows,
            );
            rows
        };

        Self::count_query(&self.query_counts, "delete:queued_instances_gc");
        let _ = sqlx::query("DELETE FROM queued_instances WHERE instance_id = ANY($1)")
            .bind(&instance_ids)
            .execute(&mut *tx)
            .await?;

        Self::count_query(&self.query_counts, "delete:runner_instances_gc");
        let deleted_instances_result =
            sqlx::query("DELETE FROM runner_instances WHERE instance_id = ANY($1)")
                .bind(&instance_ids)
                .execute(&mut *tx)
                .await?;
        let deleted_instances = deleted_instances_result.rows_affected() as usize;
        Self::count_batch_size(
            &self.batch_size_counts,
            "delete:runner_instances_gc",
            deleted_instances,
        );
        tx.commit().await?;

        Ok(GarbageCollectionResult {
            deleted_instances,
            deleted_actions,
        })
    }

    #[obs]
    async fn save_graphs_impl(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        retry_transient_backend("save_graphs_impl", || {
            let claim = claim.clone();
            async move { self.save_graphs_once(claim, graphs).await }
        })
        .await
    }

    async fn save_graphs_once(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        if graphs.is_empty() {
            return Ok(Vec::new());
        }
        let mut payloads = Vec::with_capacity(graphs.len());
        for graph in graphs {
            payloads.push((
                graph.instance_id,
                graph.next_scheduled_at(),
                claim.lock_expires_at,
                Self::serialize(graph)?,
            ));
        }

        Self::count_query(&self.query_counts, "update:queued_instances_scheduled_at");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:queued_instances_scheduled_at",
            payloads.len(),
        );
        let now = Utc::now();
        let mut schedule_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "UPDATE queued_instances AS qi SET scheduled_at = v.scheduled_at, lock_expires_at = CASE WHEN qi.lock_expires_at IS NULL OR qi.lock_expires_at < v.lock_expires_at THEN v.lock_expires_at ELSE qi.lock_expires_at END FROM (",
        );
        schedule_builder.push_values(
            payloads.iter(),
            |mut b, (instance_id, scheduled_at, lock_expires_at, _payload)| {
                b.push_bind(*instance_id)
                    .push_bind(*scheduled_at)
                    .push_bind(*lock_expires_at);
            },
        );
        schedule_builder.push(
            ") AS v(instance_id, scheduled_at, lock_expires_at)
             WHERE qi.instance_id = v.instance_id
               AND qi.lock_uuid = ",
        );
        schedule_builder.push_bind(claim.lock_uuid);
        schedule_builder.push(" AND (qi.lock_expires_at IS NULL OR qi.lock_expires_at > ");
        schedule_builder.push_bind(now);
        schedule_builder.push(")");
        schedule_builder.build().execute(&self.pool).await?;

        Self::count_query(&self.query_counts, "update:runner_instances_state");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:runner_instances_state",
            payloads.len(),
        );
        let mut runner_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("UPDATE runner_instances AS ri SET state = v.state FROM (");
        runner_builder.push_values(
            payloads.iter(),
            |mut b, (instance_id, _scheduled_at, _lock_expires_at, payload)| {
                b.push_bind(*instance_id).push_bind(payload.as_slice());
            },
        );
        runner_builder.push(
            ") AS v(instance_id, state)
             JOIN queued_instances qi ON qi.instance_id = v.instance_id
             WHERE ri.instance_id = v.instance_id
               AND qi.lock_uuid = ",
        );
        runner_builder.push_bind(claim.lock_uuid);
        runner_builder.push(" AND (qi.lock_expires_at IS NULL OR qi.lock_expires_at > ");
        runner_builder.push_bind(now);
        runner_builder.push(")");
        runner_builder.build().execute(&self.pool).await?;

        let ids: Vec<Uuid> = graphs.iter().map(|graph| graph.instance_id).collect();
        let lock_rows = sqlx::query(
            "SELECT instance_id, lock_uuid, lock_expires_at FROM queued_instances WHERE instance_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&self.pool)
        .await?;

        let mut lock_map: HashMap<Uuid, InstanceLockStatus> = HashMap::new();
        for row in lock_rows {
            let instance_id: Uuid = row.get(0);
            lock_map.insert(
                instance_id,
                InstanceLockStatus {
                    instance_id,
                    lock_uuid: row.get(1),
                    lock_expires_at: row.get(2),
                },
            );
        }

        let mut locks = Vec::with_capacity(ids.len());
        for instance_id in ids {
            locks.push(
                lock_map
                    .get(&instance_id)
                    .cloned()
                    .unwrap_or(InstanceLockStatus {
                        instance_id,
                        lock_uuid: None,
                        lock_expires_at: None,
                    }),
            );
        }
        Ok(locks)
    }

    #[obs]
    async fn save_actions_done_impl(&self, actions: &[ActionDone]) -> BackendResult<()> {
        if actions.is_empty() {
            return Ok(());
        }
        Self::count_query(&self.query_counts, "insert:runner_actions_done");
        Self::count_batch_size(
            &self.batch_size_counts,
            "insert:runner_actions_done",
            actions.len(),
        );
        let mut payloads = Vec::new();
        for action in actions {
            payloads.push((
                action.execution_id,
                action.attempt,
                action.status.to_string(),
                action.started_at,
                action.completed_at,
                action.duration_ms,
                Self::serialize(&action.result)?,
            ));
        }
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO runner_actions_done (execution_id, attempt, status, started_at, completed_at, duration_ms, result) ",
        );
        builder.push_values(
            payloads.iter(),
            |mut b, (execution_id, attempt, status, started_at, completed_at, duration_ms, payload)| {
                b.push_bind(*execution_id)
                    .push_bind(*attempt)
                    .push_bind(status.as_str())
                    .push_bind(*started_at)
                    .push_bind(*completed_at)
                    .push_bind(*duration_ms)
                    .push_bind(payload.as_slice());
            },
        );
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    #[obs]
    async fn get_queued_instances_impl(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        retry_transient_backend("get_queued_instances_impl", || {
            let claim = claim.clone();
            async move { self.get_queued_instances_once(size, claim).await }
        })
        .await
    }

    async fn get_queued_instances_once(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        if size == 0 {
            return Ok(QueuedInstanceBatch {
                instances: Vec::new(),
            });
        }
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;
        Self::count_query(&self.query_counts, "select:queued_instances");
        let rows = sqlx::query(
            r#"
            WITH claimed AS (
                SELECT instance_id, payload
                FROM queued_instances
                WHERE scheduled_at <= $1
                  AND (lock_uuid IS NULL OR lock_expires_at <= $1)
                ORDER BY scheduled_at, created_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE queued_instances AS qi
                SET lock_uuid = $3,
                    lock_expires_at = $4
                FROM claimed
                WHERE qi.instance_id = claimed.instance_id
                RETURNING qi.instance_id, claimed.payload
            )
            SELECT updated.instance_id, updated.payload, ri.state
            FROM updated
            JOIN runner_instances ri ON ri.instance_id = updated.instance_id
            "#,
        )
        .bind(now)
        .bind(size as i64)
        .bind(claim.lock_uuid)
        .bind(claim.lock_expires_at)
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            tx.commit().await?;
            return Ok(QueuedInstanceBatch {
                instances: Vec::new(),
            });
        }

        let claimed_instance_ids: Vec<Uuid> =
            rows.iter().map(|row| row.get("instance_id")).collect();
        sqlx::query("UPDATE runner_instances SET current_status = $2 WHERE instance_id = ANY($1)")
            .bind(&claimed_instance_ids)
            .bind(INSTANCE_STATUS_RUNNING)
            .execute(&mut *tx)
            .await?;

        Self::count_batch_size(
            &self.batch_size_counts,
            "select:queued_instances",
            rows.len(),
        );
        tx.commit().await?;

        let mut instances = Vec::new();
        let mut action_node_ids_by_instance: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        let mut all_action_node_ids: Vec<Uuid> = Vec::new();
        for row in rows {
            let instance_id: Uuid = row.get(0);
            let payload: Vec<u8> = row.get(1);
            let state_payload: Option<Vec<u8>> = row.get(2);
            let mut instance: QueuedInstance = Self::deserialize(&payload)?;
            instance.instance_id = instance_id;
            if let Some(state_payload) = state_payload {
                let graph: GraphUpdate = Self::deserialize(&state_payload)?;
                let action_node_ids: Vec<Uuid> = graph
                    .nodes
                    .iter()
                    .filter_map(|(node_id, node)| node.is_action_call().then_some(*node_id))
                    .collect();
                if !action_node_ids.is_empty() {
                    all_action_node_ids.extend(action_node_ids.iter().copied());
                    action_node_ids_by_instance.insert(instance_id, action_node_ids);
                }
                instance.state = Some(RunnerState::new(
                    None,
                    Some(graph.nodes),
                    Some(graph.edges),
                    false,
                ));
            }
            instances.push(instance);
        }

        if !all_action_node_ids.is_empty() {
            all_action_node_ids.sort_unstable();
            all_action_node_ids.dedup();

            Self::count_query(
                &self.query_counts,
                "select:runner_actions_done_by_execution_id",
            );
            let rows = sqlx::query(
                r#"
                SELECT DISTINCT ON (execution_id)
                    execution_id,
                    result
                FROM runner_actions_done
                WHERE execution_id = ANY($1)
                ORDER BY execution_id, attempt DESC, id DESC
                "#,
            )
            .bind(&all_action_node_ids)
            .fetch_all(&self.pool)
            .await?;

            let mut action_results_by_execution_id: HashMap<Uuid, serde_json::Value> =
                HashMap::new();
            for row in rows {
                let execution_id: Uuid = row.get("execution_id");
                let result_payload: Option<Vec<u8>> = row.get("result");
                let Some(result_payload) = result_payload else {
                    continue;
                };
                let result: serde_json::Value = Self::deserialize(&result_payload)?;
                action_results_by_execution_id.insert(execution_id, result);
            }

            for instance in &mut instances {
                let Some(action_node_ids) = action_node_ids_by_instance.get(&instance.instance_id)
                else {
                    continue;
                };
                for node_id in action_node_ids {
                    if let Some(result) = action_results_by_execution_id.get(node_id) {
                        instance.action_results.insert(*node_id, result.clone());
                    }
                }
            }
        }

        Ok(QueuedInstanceBatch { instances })
    }

    #[obs]
    async fn save_instances_done_impl(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        retry_transient_backend("save_instances_done_impl", || async move {
            self.save_instances_done_once(instances).await
        })
        .await
    }

    async fn save_instances_done_once(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        if instances.is_empty() {
            return Ok(());
        }
        let ids: Vec<Uuid> = instances
            .iter()
            .map(|instance| instance.executor_id)
            .collect();

        let mut tx = self.pool.begin().await?;
        Self::count_query(&self.query_counts, "delete:queued_instances_by_id");
        sqlx::query("DELETE FROM queued_instances WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        Self::count_query(&self.query_counts, "update:runner_instances_result");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:runner_instances_result",
            instances.len(),
        );
        let mut payloads = Vec::with_capacity(instances.len());
        for instance in instances {
            let current_status = instance_done_status(instance);
            let result = match &instance.result {
                Some(value) => Some(Self::serialize(value)?),
                None => None,
            };
            let error = match &instance.error {
                Some(value) => Some(Self::serialize(value)?),
                None => None,
            };
            payloads.push((instance.executor_id, current_status, result, error));
        }
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "UPDATE runner_instances AS ri SET result = v.result, error = v.error, current_status = v.current_status FROM (",
        );
        builder.push_values(
            payloads.iter(),
            |mut b, (instance_id, current_status, result, error)| {
                b.push_bind(*instance_id)
                    .push_bind(*current_status)
                    .push_bind(result.as_deref())
                    .push_bind(error.as_deref());
            },
        );
        builder.push(
            ") AS v(instance_id, current_status, result, error) WHERE ri.instance_id = v.instance_id",
        );
        builder.build().execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait]
impl CoreBackend for PostgresBackend {
    fn clone_box(&self) -> Box<dyn CoreBackend> {
        Box::new(self.clone())
    }

    async fn save_graphs(
        &self,
        claim: LockClaim,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.save_graphs_impl(claim, graphs).await
    }

    async fn save_actions_done(&self, actions: &[ActionDone]) -> BackendResult<()> {
        self.save_actions_done_impl(actions).await
    }

    async fn get_queued_instances(
        &self,
        size: usize,
        claim: LockClaim,
    ) -> BackendResult<QueuedInstanceBatch> {
        self.get_queued_instances_impl(size, claim).await
    }

    async fn save_instances_done(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        self.save_instances_done_impl(instances).await
    }

    async fn refresh_instance_locks(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        retry_transient_backend("refresh_instance_locks", || {
            let claim = claim.clone();
            async move { self.refresh_instance_locks_once(claim, instance_ids).await }
        })
        .await
    }

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        if instance_ids.is_empty() {
            return Ok(());
        }
        Self::count_query(&self.query_counts, "update:queued_instances_release");
        let released_rows = sqlx::query(
            r#"
            WITH releasable AS (
                SELECT instance_id
                FROM queued_instances
                WHERE instance_id = ANY($1)
                  AND lock_uuid = $2
                FOR UPDATE SKIP LOCKED
            ),
            released AS (
                UPDATE queued_instances AS qi
                SET lock_uuid = NULL,
                    lock_expires_at = NULL
                FROM releasable
                WHERE qi.instance_id = releasable.instance_id
                RETURNING qi.instance_id
            )
            SELECT instance_id FROM released
            "#,
        )
        .bind(instance_ids)
        .bind(lock_uuid)
        .fetch_all(&self.pool)
        .await?;

        if !released_rows.is_empty() {
            let released_instance_ids: Vec<Uuid> = released_rows
                .iter()
                .map(|row| row.get("instance_id"))
                .collect();
            sqlx::query(
                "UPDATE runner_instances SET current_status = $2 WHERE instance_id = ANY($1) AND result IS NULL AND error IS NULL",
            )
            .bind(&released_instance_ids)
            .bind(INSTANCE_STATUS_QUEUED)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        PostgresBackend::queue_instances(self, instances).await
    }
}

impl PostgresBackend {
    async fn refresh_instance_locks_once(
        &self,
        claim: LockClaim,
        instance_ids: &[Uuid],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        if instance_ids.is_empty() {
            return Ok(Vec::new());
        }
        Self::count_query(&self.query_counts, "update:queued_instances_lock");
        sqlx::query(
            r#"
            WITH claimable AS (
                SELECT instance_id
                FROM queued_instances
                WHERE instance_id = ANY($2)
                  AND lock_uuid = $3
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queued_instances AS qi
            SET lock_expires_at = $1
            FROM claimable
            WHERE qi.instance_id = claimable.instance_id
            "#,
        )
        .bind(claim.lock_expires_at)
        .bind(instance_ids)
        .bind(claim.lock_uuid)
        .execute(&self.pool)
        .await?;
        let rows = sqlx::query(
            "SELECT instance_id, lock_uuid, lock_expires_at FROM queued_instances WHERE instance_id = ANY($1)",
        )
        .bind(instance_ids)
        .fetch_all(&self.pool)
        .await?;
        let mut locks = Vec::with_capacity(rows.len());
        for row in rows {
            locks.push(InstanceLockStatus {
                instance_id: row.get(0),
                lock_uuid: row.get(1),
                lock_expires_at: row.get(2),
            });
        }
        Ok(locks)
    }
}

#[async_trait]
impl GarbageCollectorBackend for PostgresBackend {
    async fn collect_done_instances(
        &self,
        older_than: DateTime<Utc>,
        limit: usize,
    ) -> BackendResult<GarbageCollectionResult> {
        self.collect_done_instances_impl(older_than, limit).await
    }
}

#[async_trait]
impl WorkerStatusBackend for PostgresBackend {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        PostgresBackend::upsert_worker_status(self, status).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration as StdDuration;

    use chrono::{DateTime, Duration, Utc};
    use serial_test::serial;
    use sqlx::Row;
    use uuid::Uuid;

    use super::super::test_helpers::setup_backend;
    use super::*;
    use crate::backends::{
        ActionAttemptStatus, CoreBackend, GarbageCollectorBackend, WorkerStatusBackend,
    };
    use crate::waymark_core::dag::EdgeType;
    use crate::waymark_core::runner::state::{ActionCallSpec, ExecutionNode, NodeStatus};

    fn sample_runner_state() -> RunnerState {
        RunnerState::new(None, None, None, false)
    }

    fn sample_queued_instance(instance_id: Uuid, entry_node: Uuid) -> QueuedInstance {
        QueuedInstance {
            workflow_version_id: Uuid::new_v4(),
            schedule_id: None,
            dag: None,
            entry_node,
            state: Some(sample_runner_state()),
            action_results: HashMap::new(),
            instance_id,
            scheduled_at: Some(Utc::now() - Duration::seconds(1)),
        }
    }

    fn sample_execution_node(node_id: Uuid) -> ExecutionNode {
        ExecutionNode {
            node_id,
            node_type: "action_call".to_string(),
            label: "@tests.action()".to_string(),
            status: NodeStatus::Queued,
            template_id: Some("n0".to_string()),
            targets: Vec::new(),
            action: Some(ActionCallSpec {
                action_name: "tests.action".to_string(),
                module_name: Some("tests".to_string()),
                kwargs: HashMap::new(),
            }),
            value_expr: None,
            assignments: HashMap::new(),
            action_attempt: 1,
            started_at: None,
            completed_at: None,
            scheduled_at: Some(Utc::now() + Duration::seconds(15)),
        }
    }

    fn sample_lock_claim() -> LockClaim {
        LockClaim {
            lock_uuid: Uuid::new_v4(),
            lock_expires_at: Utc::now() + Duration::seconds(30),
        }
    }

    async fn insert_workflow_version_row(
        backend: &PostgresBackend,
        workflow_version_id: Uuid,
        workflow_name: &str,
    ) {
        sqlx::query(
            "INSERT INTO workflow_versions (id, workflow_name, workflow_version, ir_hash, program_proto, concurrent) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(workflow_version_id)
        .bind(workflow_name)
        .bind("v1")
        .bind(format!("hash-{workflow_name}"))
        .bind(vec![0_u8])
        .bind(false)
        .execute(backend.pool())
        .await
        .expect("insert workflow version row");
    }

    async fn claim_instance(backend: &PostgresBackend, instance_id: Uuid) -> LockClaim {
        let claim = sample_lock_claim();
        let batch = CoreBackend::get_queued_instances(backend, 10, claim.clone())
            .await
            .expect("claim queued instance");
        assert_eq!(batch.instances.len(), 1);
        assert_eq!(batch.instances[0].instance_id, instance_id);
        claim
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_queue_instances_happy_path() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let queued = sample_queued_instance(instance_id, entry_node);
        let expected_workflow_version_id = queued.workflow_version_id;

        CoreBackend::queue_instances(&backend, &[queued])
            .await
            .expect("queue instances");

        let queued_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM queued_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("queued count");
        assert_eq!(queued_count, 1);

        let runner_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runner_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("runner count");
        assert_eq!(runner_count, 1);

        let workflow_version_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT workflow_version_id FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner workflow version");
        assert_eq!(workflow_version_id, Some(expected_workflow_version_id));

        let runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner current status");
        assert_eq!(runner_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));

        let queued_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM queued_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("queued current status");
        assert_eq!(queued_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_queue_instances_persists_workflow_name_when_registered() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let workflow_version_id = Uuid::new_v4();
        insert_workflow_version_row(&backend, workflow_version_id, "tests.searchable").await;

        let queued = QueuedInstance {
            workflow_version_id,
            schedule_id: None,
            dag: None,
            entry_node,
            state: Some(sample_runner_state()),
            action_results: HashMap::new(),
            instance_id,
            scheduled_at: Some(Utc::now()),
        };

        CoreBackend::queue_instances(&backend, &[queued])
            .await
            .expect("queue instances");

        let runner_workflow_name: Option<String> =
            sqlx::query_scalar("SELECT workflow_name FROM runner_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("runner workflow_name");
        assert_eq!(runner_workflow_name.as_deref(), Some("tests.searchable"));

        let queued_workflow_name: Option<String> =
            sqlx::query_scalar("SELECT workflow_name FROM queued_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("queued workflow_name");
        assert_eq!(queued_workflow_name.as_deref(), Some("tests.searchable"));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_get_queued_instances_updates_runner_status_without_mutating_queue_status() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let queued = sample_queued_instance(instance_id, entry_node);
        CoreBackend::queue_instances(&backend, &[queued])
            .await
            .expect("queue instances");

        let claim = sample_lock_claim();
        let batch = CoreBackend::get_queued_instances(&backend, 1, claim.clone())
            .await
            .expect("get queued instances");
        assert_eq!(batch.instances.len(), 1);
        assert_eq!(batch.instances[0].instance_id, instance_id);

        let row = sqlx::query("SELECT lock_uuid FROM queued_instances WHERE instance_id = $1")
            .bind(instance_id)
            .fetch_one(backend.pool())
            .await
            .expect("queued lock row");
        let lock_uuid: Option<Uuid> = row.get("lock_uuid");
        assert_eq!(lock_uuid, Some(claim.lock_uuid));

        let queued_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM queued_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("queued current status");
        assert_eq!(queued_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));

        let runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner current status");
        assert_eq!(runner_status.as_deref(), Some(INSTANCE_STATUS_RUNNING));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_get_queued_instances_restores_action_results_from_actions_done() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");

        let initial_claim = sample_lock_claim();
        let initial_batch = CoreBackend::get_queued_instances(&backend, 1, initial_claim.clone())
            .await
            .expect("initial claim");
        assert_eq!(initial_batch.instances.len(), 1);

        let execution_id = Uuid::new_v4();
        let mut completed_action_node = sample_execution_node(execution_id);
        completed_action_node.status = NodeStatus::Completed;
        completed_action_node.scheduled_at = None;

        let graph = GraphUpdate {
            instance_id,
            nodes: HashMap::from([(execution_id, completed_action_node)]),
            edges: std::collections::HashSet::new(),
        };
        CoreBackend::save_graphs(
            &backend,
            initial_claim.clone(),
            std::slice::from_ref(&graph),
        )
        .await
        .expect("persist graph");

        CoreBackend::save_actions_done(
            &backend,
            &[ActionDone {
                execution_id,
                attempt: 1,
                status: ActionAttemptStatus::Completed,
                started_at: None,
                completed_at: Some(Utc::now()),
                duration_ms: None,
                result: serde_json::json!({"ok": true}),
            }],
        )
        .await
        .expect("persist action result");

        CoreBackend::release_instance_locks(&backend, initial_claim.lock_uuid, &[instance_id])
            .await
            .expect("release initial lock");

        let queued_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM queued_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("queued current status after release");
        assert_eq!(queued_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));

        let runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner current status after release");
        assert_eq!(runner_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));

        let second_claim = sample_lock_claim();
        let batch = CoreBackend::get_queued_instances(&backend, 1, second_claim)
            .await
            .expect("rehydrate instance");
        assert_eq!(batch.instances.len(), 1);
        assert_eq!(
            batch.instances[0].action_results.get(&execution_id),
            Some(&serde_json::json!({"ok": true}))
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_save_graphs_happy_path() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");
        let claim = claim_instance(&backend, instance_id).await;

        let execution_id = Uuid::new_v4();
        let mut nodes = HashMap::new();
        nodes.insert(execution_id, sample_execution_node(execution_id));
        let graph = GraphUpdate {
            instance_id,
            nodes,
            edges: std::collections::HashSet::from([
                crate::waymark_core::runner::state::ExecutionEdge {
                    source: execution_id,
                    target: execution_id,
                    edge_type: EdgeType::StateMachine,
                },
            ]),
        };
        let extended_claim = LockClaim {
            lock_uuid: claim.lock_uuid,
            lock_expires_at: claim.lock_expires_at + Duration::seconds(120),
        };

        let locks = CoreBackend::save_graphs(
            &backend,
            extended_claim.clone(),
            std::slice::from_ref(&graph),
        )
        .await
        .expect("save graphs");
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].instance_id, instance_id);
        assert_eq!(locks[0].lock_uuid, Some(claim.lock_uuid));
        assert_eq!(
            locks[0]
                .lock_expires_at
                .map(|value| value.timestamp_micros()),
            Some(extended_claim.lock_expires_at.timestamp_micros()),
        );

        let state_payload: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT state FROM runner_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("runner state payload");
        let decoded: GraphUpdate = rmp_serde::from_slice(&state_payload.expect("state payload"))
            .expect("decode graph update");
        assert_eq!(decoded.nodes.len(), 1);
        assert_eq!(decoded.edges.len(), 1);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_save_graphs_returns_lock_status_for_duplicate_instance_updates() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");
        let claim = claim_instance(&backend, instance_id).await;

        let first_node_id = Uuid::new_v4();
        let second_node_id = Uuid::new_v4();
        let first_graph = GraphUpdate {
            instance_id,
            nodes: HashMap::from([(first_node_id, sample_execution_node(first_node_id))]),
            edges: HashSet::new(),
        };
        let second_graph = GraphUpdate {
            instance_id,
            nodes: HashMap::from([(second_node_id, sample_execution_node(second_node_id))]),
            edges: HashSet::new(),
        };

        let locks = CoreBackend::save_graphs(
            &backend,
            claim.clone(),
            &[first_graph.clone(), second_graph.clone()],
        )
        .await
        .expect("save duplicate instance graphs");
        assert_eq!(locks.len(), 2);
        assert_eq!(locks[0].instance_id, instance_id);
        assert_eq!(locks[1].instance_id, instance_id);
        assert_eq!(locks[0].lock_uuid, Some(claim.lock_uuid));
        assert_eq!(locks[1].lock_uuid, Some(claim.lock_uuid));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_save_actions_done_happy_path() {
        let backend = setup_backend().await;
        let execution_id = Uuid::new_v4();
        CoreBackend::save_actions_done(
            &backend,
            &[ActionDone {
                execution_id,
                attempt: 1,
                status: ActionAttemptStatus::Completed,
                started_at: None,
                completed_at: Some(Utc::now()),
                duration_ms: None,
                result: serde_json::json!({"ok": true}),
            }],
        )
        .await
        .expect("save actions done");

        let row = sqlx::query(
            "SELECT execution_id, attempt, status, started_at, completed_at, duration_ms, result FROM runner_actions_done WHERE execution_id = $1",
        )
        .bind(execution_id)
        .fetch_one(backend.pool())
        .await
        .expect("action row");

        assert_eq!(row.get::<Uuid, _>("execution_id"), execution_id);
        assert_eq!(row.get::<i32, _>("attempt"), 1);
        assert_eq!(row.get::<String, _>("status"), "completed");
        assert!(
            row.get::<Option<DateTime<Utc>>, _>("completed_at")
                .is_some()
        );
        let payload: Option<Vec<u8>> = row.get("result");
        let decoded: serde_json::Value =
            rmp_serde::from_slice(&payload.expect("action payload")).expect("decode action");
        assert_eq!(decoded, serde_json::json!({"ok": true}));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_refresh_instance_locks_happy_path() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");
        let claim = claim_instance(&backend, instance_id).await;

        let refreshed_expiry = Utc::now() + Duration::seconds(120);
        let refreshed = CoreBackend::refresh_instance_locks(
            &backend,
            LockClaim {
                lock_uuid: claim.lock_uuid,
                lock_expires_at: refreshed_expiry,
            },
            &[instance_id],
        )
        .await
        .expect("refresh locks");

        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].instance_id, instance_id);
        assert_eq!(refreshed[0].lock_uuid, Some(claim.lock_uuid));
        assert_eq!(
            refreshed[0]
                .lock_expires_at
                .map(|value| value.timestamp_micros()),
            Some(refreshed_expiry.timestamp_micros()),
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_refresh_instance_locks_skip_locked_does_not_block_or_override() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");
        let claim = claim_instance(&backend, instance_id).await;

        let mut tx = backend.pool().begin().await.expect("begin lock tx");
        sqlx::query("SELECT instance_id FROM queued_instances WHERE instance_id = $1 FOR UPDATE")
            .bind(instance_id)
            .fetch_one(&mut *tx)
            .await
            .expect("lock queued row");

        let refreshed_expiry = Utc::now() + Duration::seconds(120);
        let refreshed = tokio::time::timeout(
            StdDuration::from_millis(300),
            CoreBackend::refresh_instance_locks(
                &backend,
                LockClaim {
                    lock_uuid: claim.lock_uuid,
                    lock_expires_at: refreshed_expiry,
                },
                &[instance_id],
            ),
        )
        .await
        .expect("refresh should not block")
        .expect("refresh locks");

        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].instance_id, instance_id);
        assert_eq!(refreshed[0].lock_uuid, Some(claim.lock_uuid));
        assert_eq!(
            refreshed[0]
                .lock_expires_at
                .map(|value| value.timestamp_micros()),
            Some(claim.lock_expires_at.timestamp_micros()),
        );
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_release_instance_locks_happy_path() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");
        let claim = claim_instance(&backend, instance_id).await;

        CoreBackend::release_instance_locks(&backend, claim.lock_uuid, &[instance_id])
            .await
            .expect("release locks");

        let row = sqlx::query(
            "SELECT lock_uuid, lock_expires_at FROM queued_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("lock row");
        let lock_uuid: Option<Uuid> = row.get("lock_uuid");
        let lock_expires_at: Option<chrono::DateTime<Utc>> = row.get("lock_expires_at");
        assert!(lock_uuid.is_none());
        assert!(lock_expires_at.is_none());

        let queued_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM queued_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("queued current status after release");
        assert_eq!(queued_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));

        let runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner current status after release");
        assert_eq!(runner_status.as_deref(), Some(INSTANCE_STATUS_QUEUED));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_reclaim_expired_instance_locks_happy_path() {
        let backend = setup_backend().await;
        let expired_id = Uuid::new_v4();
        let live_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(
            &backend,
            &[
                sample_queued_instance(expired_id, entry_node),
                sample_queued_instance(live_id, entry_node),
            ],
        )
        .await
        .expect("queue instances");

        let claim = sample_lock_claim();
        let claimed = CoreBackend::get_queued_instances(&backend, 10, claim.clone())
            .await
            .expect("claim queued instances");
        assert_eq!(claimed.instances.len(), 2);

        let expired_at = Utc::now() - Duration::seconds(1);
        let live_at = Utc::now() + Duration::seconds(60);
        sqlx::query(
            r#"
            UPDATE queued_instances
            SET lock_expires_at = CASE
                WHEN instance_id = $1 THEN $3
                ELSE $4
            END
            WHERE instance_id IN ($1, $2)
            "#,
        )
        .bind(expired_id)
        .bind(live_id)
        .bind(expired_at)
        .bind(live_at)
        .execute(backend.pool())
        .await
        .expect("set lock expiries");

        let reclaimed = backend
            .reclaim_expired_instance_locks(10)
            .await
            .expect("reclaim expired locks");
        assert_eq!(reclaimed, 1);

        let rows = sqlx::query(
            "SELECT instance_id, lock_uuid, lock_expires_at FROM queued_instances WHERE instance_id IN ($1, $2)",
        )
        .bind(expired_id)
        .bind(live_id)
        .fetch_all(backend.pool())
        .await
        .expect("fetch lock rows");
        let mut lock_rows: HashMap<Uuid, (Option<Uuid>, Option<chrono::DateTime<Utc>>)> =
            HashMap::new();
        for row in rows {
            let instance_id: Uuid = row.get("instance_id");
            let lock_uuid: Option<Uuid> = row.get("lock_uuid");
            let lock_expires_at: Option<chrono::DateTime<Utc>> = row.get("lock_expires_at");
            lock_rows.insert(instance_id, (lock_uuid, lock_expires_at));
        }

        let expired_lock = lock_rows.get(&expired_id).expect("expired lock row");
        assert_eq!(*expired_lock, (None, None));

        let expired_runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(expired_id)
        .fetch_one(backend.pool())
        .await
        .expect("expired runner status");
        assert_eq!(
            expired_runner_status.as_deref(),
            Some(INSTANCE_STATUS_QUEUED)
        );

        let live_lock = lock_rows.get(&live_id).expect("live lock row");
        assert_eq!(live_lock.0, Some(claim.lock_uuid));
        assert_eq!(
            live_lock.1.map(|value| value.timestamp_micros()),
            Some(live_at.timestamp_micros()),
        );

        let live_runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(live_id)
        .fetch_one(backend.pool())
        .await
        .expect("live runner status");
        assert_eq!(live_runner_status.as_deref(), Some(INSTANCE_STATUS_RUNNING));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_save_instances_done_happy_path() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");

        CoreBackend::save_instances_done(
            &backend,
            &[InstanceDone {
                executor_id: instance_id,
                entry_node,
                result: Some(serde_json::json!({"value": 3})),
                error: None,
            }],
        )
        .await
        .expect("save instances done");

        let result_payload: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT result FROM runner_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("result payload");
        let decoded: serde_json::Value =
            rmp_serde::from_slice(&result_payload.expect("stored result")).expect("decode result");
        assert_eq!(decoded, serde_json::json!({"value": 3}));

        let queued_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM queued_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("queued count");
        assert_eq!(queued_count, 0);

        let runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner status");
        assert_eq!(runner_status.as_deref(), Some(INSTANCE_STATUS_COMPLETED));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_save_instances_done_updates_runner_even_if_queue_row_missing() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        CoreBackend::queue_instances(&backend, &[sample_queued_instance(instance_id, entry_node)])
            .await
            .expect("queue instances");

        sqlx::query("DELETE FROM queued_instances WHERE instance_id = $1")
            .bind(instance_id)
            .execute(backend.pool())
            .await
            .expect("delete queued row");

        CoreBackend::save_instances_done(
            &backend,
            &[InstanceDone {
                executor_id: instance_id,
                entry_node,
                result: Some(serde_json::json!({"value": 11})),
                error: None,
            }],
        )
        .await
        .expect("save instances done without queue row");

        let runner_status: Option<String> = sqlx::query_scalar(
            "SELECT current_status FROM runner_instances WHERE instance_id = $1",
        )
        .bind(instance_id)
        .fetch_one(backend.pool())
        .await
        .expect("runner status");
        assert_eq!(runner_status.as_deref(), Some(INSTANCE_STATUS_COMPLETED));
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_retry_transient_deadlock_sqlstate_happy_path() {
        let backend = setup_backend().await;
        let pool = backend.pool().clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = retry_transient_backend("core_retry_test", || {
            let pool = pool.clone();
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    sqlx::query(
                        "DO $$ BEGIN RAISE EXCEPTION 'simulated deadlock' USING ERRCODE='40P01'; END $$;",
                    )
                    .execute(&pool)
                    .await?;
                }
                Ok(())
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_retry_non_transient_sqlstate_fails_without_retry() {
        let backend = setup_backend().await;
        let pool = backend.pool().clone();
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = retry_transient_backend("core_retry_non_transient_test", || {
            let pool = pool.clone();
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                sqlx::query(
                    "DO $$ BEGIN RAISE EXCEPTION 'simulated unique violation' USING ERRCODE='23505'; END $$;",
                )
                .execute(&pool)
                .await?;
                Ok::<(), BackendError>(())
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn garbage_collector_deletes_old_done_instances_and_actions() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let workflow_version_id = Uuid::new_v4();

        let state = GraphUpdate {
            instance_id,
            nodes: HashMap::from([(execution_id, sample_execution_node(execution_id))]),
            edges: HashSet::new(),
        };
        let state_payload = PostgresBackend::serialize(&state).expect("serialize state");
        let result_payload =
            PostgresBackend::serialize(&serde_json::json!({"ok": true})).expect("serialize done");
        let action_payload =
            PostgresBackend::serialize(&serde_json::json!({"value": 1})).expect("serialize action");

        sqlx::query(
            "INSERT INTO runner_instances (instance_id, entry_node, workflow_version_id, created_at, state, result) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(instance_id)
        .bind(entry_node)
        .bind(workflow_version_id)
        .bind(Utc::now() - Duration::hours(30))
        .bind(state_payload)
        .bind(result_payload)
        .execute(backend.pool())
        .await
        .expect("insert old done instance");

        sqlx::query(
            "INSERT INTO runner_actions_done (execution_id, attempt, status, result) VALUES ($1, $2, $3, $4)",
        )
        .bind(execution_id)
        .bind(1_i32)
        .bind("completed")
        .bind(action_payload)
        .execute(backend.pool())
        .await
        .expect("insert action row");

        let result = GarbageCollectorBackend::collect_done_instances(
            &backend,
            Utc::now() - Duration::hours(24),
            100,
        )
        .await
        .expect("collect done instances");

        assert_eq!(result.deleted_instances, 1);
        assert_eq!(result.deleted_actions, 1);

        let remaining_instances: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runner_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("count instances");
        assert_eq!(remaining_instances, 0);

        let remaining_actions: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runner_actions_done WHERE execution_id = $1")
                .bind(execution_id)
                .fetch_one(backend.pool())
                .await
                .expect("count actions");
        assert_eq!(remaining_actions, 0);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn garbage_collector_keeps_recent_done_instances() {
        let backend = setup_backend().await;
        let instance_id = Uuid::new_v4();
        let entry_node = Uuid::new_v4();
        let workflow_version_id = Uuid::new_v4();
        let state_payload = PostgresBackend::serialize(&GraphUpdate {
            instance_id,
            nodes: HashMap::new(),
            edges: HashSet::new(),
        })
        .expect("serialize state");
        let result_payload =
            PostgresBackend::serialize(&serde_json::json!({"ok": true})).expect("serialize done");

        sqlx::query(
            "INSERT INTO runner_instances (instance_id, entry_node, workflow_version_id, created_at, state, result) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(instance_id)
        .bind(entry_node)
        .bind(workflow_version_id)
        .bind(Utc::now() - Duration::hours(1))
        .bind(state_payload)
        .bind(result_payload)
        .execute(backend.pool())
        .await
        .expect("insert recent done instance");

        let result = GarbageCollectorBackend::collect_done_instances(
            &backend,
            Utc::now() - Duration::hours(24),
            100,
        )
        .await
        .expect("collect done instances");

        assert_eq!(result.deleted_instances, 0);
        assert_eq!(result.deleted_actions, 0);

        let remaining_instances: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM runner_instances WHERE instance_id = $1")
                .bind(instance_id)
                .fetch_one(backend.pool())
                .await
                .expect("count instances");
        assert_eq!(remaining_instances, 1);
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn worker_status_backend_upsert_worker_status_happy_path() {
        let backend = setup_backend().await;
        let pool_id = Uuid::new_v4();

        WorkerStatusBackend::upsert_worker_status(
            &backend,
            &WorkerStatusUpdate {
                pool_id,
                throughput_per_min: 180.0,
                total_completed: 20,
                last_action_at: Some(Utc::now()),
                median_dequeue_ms: Some(5),
                median_handling_ms: Some(12),
                dispatch_queue_size: 3,
                total_in_flight: 2,
                active_workers: 4,
                actions_per_sec: 3.0,
                median_instance_duration_secs: Some(0.2),
                active_instance_count: 1,
                total_instances_completed: 8,
                instances_per_sec: 0.5,
                instances_per_min: 30.0,
                time_series: None,
            },
        )
        .await
        .expect("upsert worker status");

        let row = sqlx::query(
            "SELECT total_completed, active_workers, actions_per_sec FROM worker_status WHERE pool_id = $1",
        )
        .bind(pool_id)
        .fetch_one(backend.pool())
        .await
        .expect("worker status row");
        assert_eq!(row.get::<i64, _>("total_completed"), 20);
        assert_eq!(row.get::<i32, _>("active_workers"), 4);
        assert_eq!(row.get::<f64, _>("actions_per_sec"), 3.0);
    }
}
