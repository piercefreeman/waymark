//! Postgres backend for persisting runner state and action results.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use tonic::async_trait;
use uuid::Uuid;

use crate::db;
use crate::observability::obs;
use crate::rappel_core::runner::state::{NodeStatus, RunnerState};
use crate::scheduler::compute_next_run;
use crate::scheduler::{CreateScheduleParams, ScheduleId, ScheduleType, WorkflowSchedule};
use crate::webapp::{
    ExecutionEdgeView, ExecutionGraphView, ExecutionNodeView, InstanceDetail, InstanceStatus,
    InstanceSummary, ScheduleDetail, ScheduleSummary, TimelineEntry, WorkerActionRow,
    WorkerAggregateStats, WorkerStatus,
};

use super::base::{
    ActionDone, BackendError, BackendResult, CoreBackend, GraphUpdate, InstanceDone,
    InstanceLockStatus, LockClaim, QueuedInstance, QueuedInstanceBatch, SchedulerBackend,
    WebappBackend, WorkerStatusBackend, WorkerStatusUpdate, WorkflowRegistration,
    WorkflowRegistryBackend, WorkflowVersion,
};

/// Persist runner state and action results in Postgres.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    query_counts: Arc<Mutex<HashMap<String, usize>>>,
    batch_size_counts: Arc<Mutex<HashMap<String, HashMap<usize, usize>>>>,
}

impl PostgresBackend {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            query_counts: Arc::new(Mutex::new(HashMap::new())),
            batch_size_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[obs]
    pub async fn connect(dsn: &str) -> BackendResult<Self> {
        let pool = PgPool::connect(dsn).await?;
        db::run_migrations(&pool).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert queued instances for run-loop consumption.
    #[obs]
    pub async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        if instances.is_empty() {
            return Ok(());
        }
        let mut queued_payloads = Vec::new();
        let mut runner_payloads = Vec::new();
        for instance in instances {
            let state = instance.state.as_ref().ok_or_else(|| {
                BackendError::Message("queued instance missing runner state".to_string())
            })?;
            let scheduled_at = instance.scheduled_at.unwrap_or_else(Utc::now);
            let mut payload_instance = instance.clone();
            payload_instance.scheduled_at = Some(scheduled_at);
            queued_payloads.push((
                payload_instance.instance_id,
                scheduled_at,
                Self::serialize(&payload_instance)?,
            ));
            let graph = GraphUpdate::from_state(instance.instance_id, state);
            runner_payloads.push((
                instance.instance_id,
                instance.entry_node,
                Self::serialize(&graph)?,
            ));
        }

        let mut queued_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO queued_instances (instance_id, scheduled_at, payload) ");
        queued_builder.push_values(
            queued_payloads.iter(),
            |mut builder, (id, scheduled_at, payload)| {
                builder
                    .push_bind(*id)
                    .push_bind(*scheduled_at)
                    .push_bind(payload.as_slice());
            },
        );

        let mut runner_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO runner_instances (instance_id, entry_node, state) ");
        runner_builder.push_values(
            runner_payloads.iter(),
            |mut builder, (id, entry, payload)| {
                builder
                    .push_bind(*id)
                    .push_bind(*entry)
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

    /// Delete all queued instances from the backing table.
    #[obs]
    pub async fn clear_queue(&self) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "delete:queued_instances_all");
        sqlx::query("DELETE FROM queued_instances")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete all persisted runner data for a clean benchmark run.
    #[obs]
    pub async fn clear_all(&self) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "truncate:runner_tables");
        sqlx::query(
            r#"
            TRUNCATE runner_graph_updates,
                     runner_actions_done,
                     runner_instances,
                     runner_instances_done,
                     queued_instances
            RESTART IDENTITY
            "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub fn query_counts(&self) -> HashMap<String, usize> {
        self.query_counts
            .lock()
            .expect("query counts poisoned")
            .clone()
    }

    pub fn batch_size_counts(&self) -> HashMap<String, HashMap<usize, usize>> {
        self.batch_size_counts
            .lock()
            .expect("batch size counts poisoned")
            .clone()
    }

    fn count_query(counts: &Arc<Mutex<HashMap<String, usize>>>, label: &str) {
        let mut guard = counts.lock().expect("query counts poisoned");
        *guard.entry(label.to_string()).or_insert(0) += 1;
    }

    fn count_batch_size(
        counts: &Arc<Mutex<HashMap<String, HashMap<usize, usize>>>>,
        label: &str,
        size: usize,
    ) {
        if size == 0 {
            return;
        }
        let mut guard = counts.lock().expect("batch size counts poisoned");
        let entry = guard.entry(label.to_string()).or_default();
        *entry.entry(size).or_insert(0) += 1;
    }

    fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, BackendError> {
        rmp_serde::to_vec_named(value).map_err(|e| BackendError::Message(e.to_string()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, BackendError> {
        rmp_serde::from_slice(payload).map_err(|e| BackendError::Message(e.to_string()))
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

    #[obs]
    async fn save_graphs_impl(
        &self,
        lock_uuid: Uuid,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        if graphs.is_empty() {
            return Ok(Vec::new());
        }
        Self::count_query(&self.query_counts, "update:runner_instances_state");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:runner_instances_state",
            graphs.len(),
        );
        let mut payloads = Vec::with_capacity(graphs.len());
        for graph in graphs {
            payloads.push((
                graph.instance_id,
                graph.next_scheduled_at(),
                Self::serialize(graph)?,
            ));
        }
        let now = Utc::now();
        let mut builder: QueryBuilder<Postgres> =
            QueryBuilder::new("UPDATE runner_instances AS ri SET state = v.state FROM (");
        builder.push_values(
            payloads.iter(),
            |mut b, (instance_id, _scheduled_at, payload)| {
                b.push_bind(*instance_id).push_bind(payload.as_slice());
            },
        );
        builder.push(
            ") AS v(instance_id, state) \
             JOIN queued_instances qi ON qi.instance_id = v.instance_id \
             WHERE ri.instance_id = v.instance_id \
               AND qi.lock_uuid = ",
        );
        builder.push_bind(lock_uuid);
        builder.push(" AND (qi.lock_expires_at IS NULL OR qi.lock_expires_at > ");
        builder.push_bind(now);
        builder.push(")");
        builder.build().execute(&self.pool).await?;

        Self::count_query(&self.query_counts, "update:queued_instances_scheduled_at");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:queued_instances_scheduled_at",
            payloads.len(),
        );
        let mut schedule_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "UPDATE queued_instances AS qi SET scheduled_at = v.scheduled_at FROM (",
        );
        schedule_builder.push_values(
            payloads.iter(),
            |mut b, (instance_id, scheduled_at, _payload)| {
                b.push_bind(*instance_id).push_bind(*scheduled_at);
            },
        );
        schedule_builder.push(
            ") AS v(instance_id, scheduled_at) \
             WHERE qi.instance_id = v.instance_id \
               AND qi.lock_uuid = ",
        );
        schedule_builder.push_bind(lock_uuid);
        schedule_builder.push(" AND (qi.lock_expires_at IS NULL OR qi.lock_expires_at > ");
        schedule_builder.push_bind(now);
        schedule_builder.push(")");
        schedule_builder.build().execute(&self.pool).await?;

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
            locks.push(lock_map.remove(&instance_id).unwrap_or(InstanceLockStatus {
                instance_id,
                lock_uuid: None,
                lock_expires_at: None,
            }));
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
                Self::serialize(&action.result)?,
            ));
        }
        let mut builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO runner_actions_done (execution_id, attempt, result) ");
        builder.push_values(
            payloads.iter(),
            |mut b, (execution_id, attempt, payload)| {
                b.push_bind(*execution_id)
                    .push_bind(*attempt)
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
                SET lock_uuid = $3, lock_expires_at = $4
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

        Self::count_batch_size(
            &self.batch_size_counts,
            "select:queued_instances",
            rows.len(),
        );
        tx.commit().await?;

        let mut instances = Vec::new();
        for row in rows {
            let instance_id: Uuid = row.get(0);
            let payload: Vec<u8> = row.get(1);
            let state_payload: Option<Vec<u8>> = row.get(2);
            let mut instance: QueuedInstance = Self::deserialize(&payload)?;
            instance.instance_id = instance_id;
            if let Some(state_payload) = state_payload {
                let graph: GraphUpdate = Self::deserialize(&state_payload)?;
                instance.state = Some(RunnerState::new(
                    None,
                    Some(graph.nodes),
                    Some(graph.edges),
                    false,
                ));
            }
            instances.push(instance);
        }

        Ok(QueuedInstanceBatch { instances })
    }

    #[obs]
    async fn save_instances_done_impl(&self, instances: &[InstanceDone]) -> BackendResult<()> {
        if instances.is_empty() {
            return Ok(());
        }
        Self::count_query(&self.query_counts, "update:runner_instances_result");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:runner_instances_result",
            instances.len(),
        );
        let mut payloads = Vec::with_capacity(instances.len());
        for instance in instances {
            let result = match &instance.result {
                Some(value) => Some(Self::serialize(value)?),
                None => None,
            };
            let error = match &instance.error {
                Some(value) => Some(Self::serialize(value)?),
                None => None,
            };
            payloads.push((instance.executor_id, result, error));
        }
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "UPDATE runner_instances AS ri SET result = v.result, error = v.error FROM (",
        );
        builder.push_values(payloads.iter(), |mut b, (instance_id, result, error)| {
            b.push_bind(*instance_id)
                .push_bind(result.as_deref())
                .push_bind(error.as_deref());
        });
        builder.push(") AS v(instance_id, result, error) WHERE ri.instance_id = v.instance_id");
        builder.build().execute(&self.pool).await?;
        let ids: Vec<Uuid> = instances
            .iter()
            .map(|instance| instance.executor_id)
            .collect();
        Self::count_query(&self.query_counts, "delete:queued_instances_by_id");
        sqlx::query("DELETE FROM queued_instances WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&self.pool)
            .await?;
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
        lock_uuid: Uuid,
        graphs: &[GraphUpdate],
    ) -> BackendResult<Vec<InstanceLockStatus>> {
        self.save_graphs_impl(lock_uuid, graphs).await
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
        if instance_ids.is_empty() {
            return Ok(Vec::new());
        }
        Self::count_query(&self.query_counts, "update:queued_instances_lock");
        sqlx::query(
            "UPDATE queued_instances SET lock_expires_at = $1 WHERE instance_id = ANY($2) AND lock_uuid = $3",
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

    async fn release_instance_locks(
        &self,
        lock_uuid: Uuid,
        instance_ids: &[Uuid],
    ) -> BackendResult<()> {
        if instance_ids.is_empty() {
            return Ok(());
        }
        Self::count_query(&self.query_counts, "update:queued_instances_release");
        sqlx::query(
            "UPDATE queued_instances SET lock_uuid = NULL, lock_expires_at = NULL WHERE instance_id = ANY($1) AND lock_uuid = $2",
        )
        .bind(instance_ids)
        .bind(lock_uuid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn queue_instances(&self, instances: &[QueuedInstance]) -> BackendResult<()> {
        PostgresBackend::queue_instances(self, instances).await
    }
}

#[async_trait]
impl WorkerStatusBackend for PostgresBackend {
    async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()> {
        PostgresBackend::upsert_worker_status(self, status).await
    }
}

#[async_trait]
impl WorkflowRegistryBackend for PostgresBackend {
    async fn upsert_workflow_version(
        &self,
        registration: &WorkflowRegistration,
    ) -> BackendResult<Uuid> {
        let inserted = sqlx::query(
            r#"
            INSERT INTO workflow_versions
                (workflow_name, workflow_version, ir_hash, program_proto, concurrent)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (workflow_name, workflow_version)
            DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&registration.workflow_name)
        .bind(&registration.workflow_version)
        .bind(&registration.ir_hash)
        .bind(&registration.program_proto)
        .bind(registration.concurrent)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = inserted {
            let id: Uuid = row.get("id");
            return Ok(id);
        }

        let row = sqlx::query(
            r#"
            SELECT id, ir_hash
            FROM workflow_versions
            WHERE workflow_name = $1 AND workflow_version = $2
            "#,
        )
        .bind(&registration.workflow_name)
        .bind(&registration.workflow_version)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        let existing_hash: String = row.get("ir_hash");
        if existing_hash != registration.ir_hash {
            return Err(BackendError::Message(format!(
                "workflow version already exists with different IR hash: {}@{}",
                registration.workflow_name, registration.workflow_version
            )));
        }

        Ok(id)
    }

    async fn get_workflow_versions(&self, ids: &[Uuid]) -> BackendResult<Vec<WorkflowVersion>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_name, workflow_version, ir_hash, program_proto, concurrent
            FROM workflow_versions
            WHERE id = ANY($1)
            "#,
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;

        let mut versions = Vec::with_capacity(rows.len());
        for row in rows {
            versions.push(WorkflowVersion {
                id: row.get("id"),
                workflow_name: row.get("workflow_name"),
                workflow_version: row.get("workflow_version"),
                ir_hash: row.get("ir_hash"),
                program_proto: row.get("program_proto"),
                concurrent: row.get("concurrent"),
            });
        }
        Ok(versions)
    }
}

#[async_trait]
impl SchedulerBackend for PostgresBackend {
    async fn upsert_schedule(&self, params: &CreateScheduleParams) -> BackendResult<ScheduleId> {
        let next_run_at = compute_next_run(
            params.schedule_type,
            params.cron_expression.as_deref(),
            params.interval_seconds,
            params.jitter_seconds,
            None,
        )
        .map_err(BackendError::Message)?;

        let row = sqlx::query(
            r#"
            INSERT INTO workflow_schedules
                (workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                 jitter_seconds, input_payload, next_run_at, priority, allow_duplicate)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (workflow_name, schedule_name)
            DO UPDATE SET
                schedule_type = EXCLUDED.schedule_type,
                cron_expression = EXCLUDED.cron_expression,
                interval_seconds = EXCLUDED.interval_seconds,
                jitter_seconds = EXCLUDED.jitter_seconds,
                input_payload = EXCLUDED.input_payload,
                next_run_at = EXCLUDED.next_run_at,
                priority = EXCLUDED.priority,
                allow_duplicate = EXCLUDED.allow_duplicate,
                status = 'active',
                updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(&params.workflow_name)
        .bind(&params.schedule_name)
        .bind(params.schedule_type.as_str())
        .bind(&params.cron_expression)
        .bind(params.interval_seconds)
        .bind(params.jitter_seconds)
        .bind(&params.input_payload)
        .bind(next_run_at)
        .bind(params.priority)
        .bind(params.allow_duplicate)
        .fetch_one(&self.pool)
        .await?;

        let id: Uuid = row.get("id");
        Ok(ScheduleId(id))
    }

    async fn get_schedule(&self, id: ScheduleId) -> BackendResult<WorkflowSchedule> {
        let schedule = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BackendError::Message(format!("schedule not found: {}", id)))?;

        Ok(schedule.into())
    }

    async fn get_schedule_by_name(
        &self,
        workflow_name: &str,
        schedule_name: &str,
    ) -> BackendResult<Option<WorkflowSchedule>> {
        let schedule = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE workflow_name = $1 AND schedule_name = $2 AND status != 'deleted'
            "#,
        )
        .bind(workflow_name)
        .bind(schedule_name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(schedule.map(|s| s.into()))
    }

    async fn list_schedules(
        &self,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<WorkflowSchedule>> {
        let rows = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY workflow_name, schedule_name
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    async fn count_schedules(&self) -> BackendResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_schedules WHERE status != 'deleted'",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    async fn update_schedule_status(&self, id: ScheduleId, status: &str) -> BackendResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_schedule(&self, id: ScheduleId) -> BackendResult<bool> {
        SchedulerBackend::update_schedule_status(self, id, "deleted").await
    }

    async fn find_due_schedules(&self, limit: i32) -> BackendResult<Vec<WorkflowSchedule>> {
        let rows = sqlx::query_as::<_, ScheduleRow>(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE status = 'active'
              AND next_run_at IS NOT NULL
              AND next_run_at <= NOW()
            ORDER BY next_run_at
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    async fn has_running_instance(&self, _schedule_id: ScheduleId) -> BackendResult<bool> {
        Ok(false)
    }

    async fn mark_schedule_executed(
        &self,
        schedule_id: ScheduleId,
        instance_id: Uuid,
    ) -> BackendResult<()> {
        let schedule = SchedulerBackend::get_schedule(self, schedule_id).await?;
        let schedule_type = ScheduleType::parse(&schedule.schedule_type)
            .ok_or_else(|| BackendError::Message("invalid schedule type".to_string()))?;
        let next_run_at = compute_next_run(
            schedule_type,
            schedule.cron_expression.as_deref(),
            schedule.interval_seconds,
            schedule.jitter_seconds,
            Some(Utc::now()),
        )
        .map_err(BackendError::Message)?;

        sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET last_run_at = NOW(),
                last_instance_id = $2,
                next_run_at = $3,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id.0)
        .bind(instance_id)
        .bind(next_run_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn skip_schedule_run(&self, schedule_id: ScheduleId) -> BackendResult<()> {
        let schedule = SchedulerBackend::get_schedule(self, schedule_id).await?;
        let schedule_type = ScheduleType::parse(&schedule.schedule_type)
            .ok_or_else(|| BackendError::Message("invalid schedule type".to_string()))?;
        let next_run_at = compute_next_run(
            schedule_type,
            schedule.cron_expression.as_deref(),
            schedule.interval_seconds,
            schedule.jitter_seconds,
            Some(Utc::now()),
        )
        .map_err(BackendError::Message)?;

        sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET next_run_at = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id.0)
        .bind(next_run_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl WebappBackend for PostgresBackend {
    async fn count_instances(&self, search: Option<&str>) -> BackendResult<i64> {
        let count = if let Some(_search) = search {
            // For now, simple search not implemented - would need to decode state
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runner_instances")
                .fetch_one(&self.pool)
                .await?
        } else {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM runner_instances")
                .fetch_one(&self.pool)
                .await?
        };
        Ok(count)
    }

    async fn list_instances(
        &self,
        _search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> BackendResult<Vec<InstanceSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT instance_id, entry_node, created_at, state, result, error
            FROM runner_instances
            ORDER BY created_at DESC, instance_id DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut instances = Vec::new();
        for row in rows {
            let instance_id: Uuid = row.get("instance_id");
            let entry_node: Uuid = row.get("entry_node");
            let created_at: DateTime<Utc> = row.get("created_at");
            let state_bytes: Option<Vec<u8>> = row.get("state");
            let result_bytes: Option<Vec<u8>> = row.get("result");
            let error_bytes: Option<Vec<u8>> = row.get("error");

            let status = determine_status(&state_bytes, &result_bytes, &error_bytes);
            let workflow_name = extract_workflow_name(&state_bytes);
            let input_preview = extract_input_preview(&state_bytes);

            instances.push(InstanceSummary {
                id: instance_id,
                entry_node,
                created_at,
                status,
                workflow_name,
                input_preview,
            });
        }

        Ok(instances)
    }

    async fn get_instance(&self, instance_id: Uuid) -> BackendResult<InstanceDetail> {
        let row = sqlx::query(
            r#"
            SELECT instance_id, entry_node, created_at, state, result, error
            FROM runner_instances
            WHERE instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BackendError::Message(format!("instance not found: {}", instance_id)))?;

        let instance_id: Uuid = row.get("instance_id");
        let entry_node: Uuid = row.get("entry_node");
        let created_at: DateTime<Utc> = row.get("created_at");
        let state_bytes: Option<Vec<u8>> = row.get("state");
        let result_bytes: Option<Vec<u8>> = row.get("result");
        let error_bytes: Option<Vec<u8>> = row.get("error");

        let status = determine_status(&state_bytes, &result_bytes, &error_bytes);
        let workflow_name = extract_workflow_name(&state_bytes);
        let input_payload = format_state_preview(&state_bytes);
        let result_payload = format_result(&result_bytes);
        let error_payload = format_error(&error_bytes);

        Ok(InstanceDetail {
            id: instance_id,
            entry_node,
            created_at,
            status,
            workflow_name,
            input_payload,
            result_payload,
            error_payload,
        })
    }

    async fn get_execution_graph(
        &self,
        instance_id: Uuid,
    ) -> BackendResult<Option<ExecutionGraphView>> {
        let row = sqlx::query(
            r#"
            SELECT state FROM runner_instances WHERE instance_id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let state_bytes: Option<Vec<u8>> = row.get("state");
        let Some(state_bytes) = state_bytes else {
            return Ok(None);
        };

        let graph_update: GraphUpdate = rmp_serde::from_slice(&state_bytes)
            .map_err(|e| BackendError::Message(format!("failed to decode state: {}", e)))?;

        let nodes: Vec<ExecutionNodeView> = graph_update
            .nodes
            .values()
            .map(|node| ExecutionNodeView {
                id: node.node_id.to_string(),
                node_type: node.node_type.clone(),
                label: node.label.clone(),
                status: format_node_status(&node.status),
                action_name: node.action.as_ref().map(|a| a.action_name.clone()),
                module_name: node.action.as_ref().and_then(|a| a.module_name.clone()),
            })
            .collect();

        let edges: Vec<ExecutionEdgeView> = graph_update
            .edges
            .iter()
            .map(|edge| ExecutionEdgeView {
                source: edge.source.to_string(),
                target: edge.target.to_string(),
                edge_type: format!("{:?}", edge.edge_type),
            })
            .collect();

        Ok(Some(ExecutionGraphView { nodes, edges }))
    }

    async fn get_action_results(&self, instance_id: Uuid) -> BackendResult<Vec<TimelineEntry>> {
        let graph = self.get_execution_graph(instance_id).await?;
        let node_map: std::collections::HashMap<String, &ExecutionNodeView> = graph
            .as_ref()
            .map(|g| g.nodes.iter().map(|n| (n.id.clone(), n)).collect())
            .unwrap_or_default();

        let execution_ids: Vec<Uuid> = graph
            .as_ref()
            .map(|g| {
                g.nodes
                    .iter()
                    .filter_map(|n| Uuid::parse_str(&n.id).ok())
                    .collect()
            })
            .unwrap_or_default();

        if execution_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT id, created_at, execution_id, attempt, result
            FROM runner_actions_done
            WHERE execution_id = ANY($1)
            ORDER BY created_at ASC
            "#,
        )
        .bind(&execution_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut entries = Vec::new();
        for row in rows {
            let execution_id: Uuid = row.get("execution_id");
            let attempt: i32 = row.get("attempt");
            let created_at: DateTime<Utc> = row.get("created_at");
            let result_bytes: Option<Vec<u8>> = row.get("result");

            let node_id_str = execution_id.to_string();
            let node = node_map.get(&node_id_str);
            let action_name = node
                .and_then(|entry| entry.action_name.clone())
                .unwrap_or_default();

            let (response_preview, error) = if let Some(bytes) = &result_bytes {
                match rmp_serde::from_slice::<serde_json::Value>(bytes) {
                    Ok(value) => {
                        let preview = serde_json::to_string_pretty(&value)
                            .unwrap_or_else(|_| "{}".to_string());
                        if value.get("error").is_some() {
                            let err = value
                                .get("error")
                                .and_then(|e| e.as_str())
                                .map(|s| s.to_string());
                            (preview, err)
                        } else {
                            (preview, None)
                        }
                    }
                    Err(_) => ("(decode error)".to_string(), None),
                }
            } else {
                ("(no result)".to_string(), None)
            };

            let status = if error.is_some() {
                "failed".to_string()
            } else {
                "completed".to_string()
            };

            entries.push(TimelineEntry {
                action_id: node_id_str,
                action_name,
                module_name: node.and_then(|n| n.module_name.clone()),
                status,
                attempt_number: attempt,
                dispatched_at: Some(created_at.to_rfc3339()),
                completed_at: Some(created_at.to_rfc3339()),
                duration_ms: None,
                request_preview: "{}".to_string(),
                response_preview,
                error,
            });
        }

        Ok(entries)
    }

    async fn get_distinct_workflows(&self) -> BackendResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn get_distinct_statuses(&self) -> BackendResult<Vec<String>> {
        Ok(vec![
            "queued".to_string(),
            "running".to_string(),
            "completed".to_string(),
            "failed".to_string(),
        ])
    }

    async fn count_schedules(&self) -> BackendResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_schedules WHERE status != 'deleted'",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    async fn list_schedules(&self, limit: i64, offset: i64) -> BackendResult<Vec<ScheduleSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   status, next_run_at, last_run_at, created_at
            FROM workflow_schedules
            WHERE status != 'deleted'
            ORDER BY workflow_name, schedule_name
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let mut schedules = Vec::new();
        for row in rows {
            schedules.push(ScheduleSummary {
                id: row.get::<Uuid, _>("id").to_string(),
                workflow_name: row.get("workflow_name"),
                schedule_name: row.get("schedule_name"),
                schedule_type: row.get("schedule_type"),
                cron_expression: row.get("cron_expression"),
                interval_seconds: row.get("interval_seconds"),
                status: row.get("status"),
                next_run_at: row
                    .get::<Option<DateTime<Utc>>, _>("next_run_at")
                    .map(|dt| dt.to_rfc3339()),
                last_run_at: row
                    .get::<Option<DateTime<Utc>>, _>("last_run_at")
                    .map(|dt| dt.to_rfc3339()),
                created_at: row.get::<DateTime<Utc>, _>("created_at").to_rfc3339(),
            });
        }

        Ok(schedules)
    }

    async fn get_schedule(&self, schedule_id: Uuid) -> BackendResult<ScheduleDetail> {
        let row = sqlx::query(
            r#"
            SELECT id, workflow_name, schedule_name, schedule_type, cron_expression, interval_seconds,
                   jitter_seconds, input_payload, status, next_run_at, last_run_at, last_instance_id,
                   created_at, updated_at, priority, allow_duplicate
            FROM workflow_schedules
            WHERE id = $1
            "#,
        )
        .bind(schedule_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| BackendError::Message(format!("schedule not found: {}", schedule_id)))?;

        let input_payload: Option<String> = row
            .get::<Option<Vec<u8>>, _>("input_payload")
            .and_then(|bytes| {
                rmp_serde::from_slice::<serde_json::Value>(&bytes)
                    .ok()
                    .map(|v| serde_json::to_string_pretty(&v).unwrap_or_default())
            });

        Ok(ScheduleDetail {
            id: row.get::<Uuid, _>("id").to_string(),
            workflow_name: row.get("workflow_name"),
            schedule_name: row.get("schedule_name"),
            schedule_type: row.get("schedule_type"),
            cron_expression: row.get("cron_expression"),
            interval_seconds: row.get("interval_seconds"),
            jitter_seconds: row.get("jitter_seconds"),
            status: row.get("status"),
            next_run_at: row
                .get::<Option<DateTime<Utc>>, _>("next_run_at")
                .map(|dt| dt.to_rfc3339()),
            last_run_at: row
                .get::<Option<DateTime<Utc>>, _>("last_run_at")
                .map(|dt| dt.to_rfc3339()),
            last_instance_id: row
                .get::<Option<Uuid>, _>("last_instance_id")
                .map(|id| id.to_string()),
            created_at: row.get::<DateTime<Utc>, _>("created_at").to_rfc3339(),
            updated_at: row.get::<DateTime<Utc>, _>("updated_at").to_rfc3339(),
            priority: row.get("priority"),
            allow_duplicate: row.get("allow_duplicate"),
            input_payload,
        })
    }

    async fn update_schedule_status(&self, schedule_id: Uuid, status: &str) -> BackendResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE workflow_schedules
            SET status = $2, updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(schedule_id)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn get_distinct_schedule_statuses(&self) -> BackendResult<Vec<String>> {
        Ok(vec!["active".to_string(), "paused".to_string()])
    }

    async fn get_distinct_schedule_types(&self) -> BackendResult<Vec<String>> {
        Ok(vec!["cron".to_string(), "interval".to_string()])
    }

    async fn get_worker_action_stats(
        &self,
        window_minutes: i64,
    ) -> BackendResult<Vec<WorkerActionRow>> {
        let rows = sqlx::query(
            r#"
            SELECT
                pool_id,
                COUNT(DISTINCT worker_id) as active_workers,
                SUM(throughput_per_min) / 60.0 as actions_per_sec,
                SUM(throughput_per_min) as throughput_per_min,
                COALESCE(SUM(total_completed), 0)::BIGINT as total_completed,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_dequeue_ms) as median_dequeue_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_handling_ms) as median_handling_ms,
                MAX(last_action_at) as last_action_at,
                MAX(updated_at) as updated_at
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            GROUP BY pool_id
            ORDER BY actions_per_sec DESC
            "#,
        )
        .bind(window_minutes)
        .fetch_all(&self.pool)
        .await?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(WorkerActionRow {
                pool_id: row.get::<Uuid, _>("pool_id").to_string(),
                active_workers: row.get::<i64, _>("active_workers"),
                actions_per_sec: format!("{:.1}", row.get::<f64, _>("actions_per_sec")),
                throughput_per_min: row.get::<f64, _>("throughput_per_min") as i64,
                total_completed: row.get::<i64, _>("total_completed"),
                median_dequeue_ms: row
                    .get::<Option<f64>, _>("median_dequeue_ms")
                    .map(|v| v as i64),
                median_handling_ms: row
                    .get::<Option<f64>, _>("median_handling_ms")
                    .map(|v| v as i64),
                last_action_at: row
                    .get::<Option<DateTime<Utc>>, _>("last_action_at")
                    .map(|dt| dt.to_rfc3339()),
                updated_at: row.get::<DateTime<Utc>, _>("updated_at").to_rfc3339(),
            });
        }

        Ok(stats)
    }

    async fn get_worker_aggregate_stats(
        &self,
        window_minutes: i64,
    ) -> BackendResult<WorkerAggregateStats> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(DISTINCT worker_id) as active_worker_count,
                COALESCE(SUM(throughput_per_min) / 60.0, 0) as actions_per_sec,
                COALESCE(SUM(total_in_flight), 0) as total_in_flight,
                COALESCE(SUM(dispatch_queue_size), 0) as total_queue_depth
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            "#,
        )
        .bind(window_minutes)
        .fetch_one(&self.pool)
        .await?;

        Ok(WorkerAggregateStats {
            active_worker_count: row.get::<i64, _>("active_worker_count"),
            actions_per_sec: format!("{:.1}", row.get::<f64, _>("actions_per_sec")),
            total_in_flight: row.get::<i64, _>("total_in_flight"),
            total_queue_depth: row.get::<i64, _>("total_queue_depth"),
        })
    }

    async fn worker_status_table_exists(&self) -> bool {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'worker_status'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false)
    }

    async fn schedules_table_exists(&self) -> bool {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'workflow_schedules'
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false)
    }

    async fn get_worker_statuses(&self, window_minutes: i64) -> BackendResult<Vec<WorkerStatus>> {
        let rows = sqlx::query(
            r#"
            SELECT
                pool_id,
                MAX(active_workers) as active_workers,
                SUM(throughput_per_min) as throughput_per_min,
                SUM(throughput_per_min) / 60.0 as actions_per_sec,
                SUM(total_completed) as total_completed,
                MAX(last_action_at) as last_action_at,
                MAX(updated_at) as updated_at,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_dequeue_ms) as median_dequeue_ms,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_handling_ms) as median_handling_ms,
                MAX(dispatch_queue_size) as dispatch_queue_size,
                MAX(total_in_flight) as total_in_flight,
                MAX(median_instance_duration_secs) as median_instance_duration_secs,
                MAX(active_instance_count) as active_instance_count,
                COALESCE(SUM(total_instances_completed), 0)::BIGINT as total_instances_completed,
                MAX(instances_per_sec) as instances_per_sec,
                MAX(instances_per_min) as instances_per_min,
                (SELECT time_series FROM worker_status ws2
                 WHERE ws2.pool_id = worker_status.pool_id
                 AND ws2.time_series IS NOT NULL
                 ORDER BY ws2.updated_at DESC LIMIT 1) as time_series
            FROM worker_status
            WHERE updated_at > NOW() - INTERVAL '1 minute' * $1
            GROUP BY pool_id
            ORDER BY actions_per_sec DESC
            "#,
        )
        .bind(window_minutes)
        .fetch_all(&self.pool)
        .await?;

        let mut statuses = Vec::new();
        for row in rows {
            statuses.push(WorkerStatus {
                pool_id: row.get::<Uuid, _>("pool_id"),
                active_workers: row.get::<Option<i32>, _>("active_workers").unwrap_or(0),
                throughput_per_min: row.get::<f64, _>("throughput_per_min"),
                actions_per_sec: row.get::<f64, _>("actions_per_sec"),
                total_completed: row.get::<i64, _>("total_completed"),
                last_action_at: row.get::<Option<DateTime<Utc>>, _>("last_action_at"),
                updated_at: row.get::<DateTime<Utc>, _>("updated_at"),
                median_dequeue_ms: row
                    .get::<Option<f64>, _>("median_dequeue_ms")
                    .map(|v| v as i64),
                median_handling_ms: row
                    .get::<Option<f64>, _>("median_handling_ms")
                    .map(|v| v as i64),
                dispatch_queue_size: row.get::<Option<i64>, _>("dispatch_queue_size"),
                total_in_flight: row.get::<Option<i64>, _>("total_in_flight"),
                median_instance_duration_secs: row
                    .get::<Option<f64>, _>("median_instance_duration_secs"),
                active_instance_count: row
                    .get::<Option<i32>, _>("active_instance_count")
                    .unwrap_or(0),
                total_instances_completed: row
                    .get::<Option<i64>, _>("total_instances_completed")
                    .unwrap_or(0),
                instances_per_sec: row
                    .get::<Option<f64>, _>("instances_per_sec")
                    .unwrap_or(0.0),
                instances_per_min: row
                    .get::<Option<f64>, _>("instances_per_min")
                    .unwrap_or(0.0),
                time_series: row.get::<Option<Vec<u8>>, _>("time_series"),
            });
        }

        Ok(statuses)
    }
}

#[derive(sqlx::FromRow)]
struct ScheduleRow {
    id: Uuid,
    workflow_name: String,
    schedule_name: String,
    schedule_type: String,
    cron_expression: Option<String>,
    interval_seconds: Option<i64>,
    jitter_seconds: i64,
    input_payload: Option<Vec<u8>>,
    status: String,
    next_run_at: Option<DateTime<Utc>>,
    last_run_at: Option<DateTime<Utc>>,
    last_instance_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    priority: i32,
    allow_duplicate: bool,
}

impl From<ScheduleRow> for WorkflowSchedule {
    fn from(row: ScheduleRow) -> Self {
        Self {
            id: row.id,
            workflow_name: row.workflow_name,
            schedule_name: row.schedule_name,
            schedule_type: row.schedule_type,
            cron_expression: row.cron_expression,
            interval_seconds: row.interval_seconds,
            jitter_seconds: row.jitter_seconds,
            input_payload: row.input_payload,
            status: row.status,
            next_run_at: row.next_run_at,
            last_run_at: row.last_run_at,
            last_instance_id: row.last_instance_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            priority: row.priority,
            allow_duplicate: row.allow_duplicate,
        }
    }
}

fn determine_status(
    state_bytes: &Option<Vec<u8>>,
    result_bytes: &Option<Vec<u8>>,
    error_bytes: &Option<Vec<u8>>,
) -> InstanceStatus {
    if error_bytes.is_some() {
        return InstanceStatus::Failed;
    }
    if result_bytes.is_some() {
        return InstanceStatus::Completed;
    }
    if state_bytes.is_some() {
        return InstanceStatus::Running;
    }
    InstanceStatus::Queued
}

fn extract_workflow_name(state_bytes: &Option<Vec<u8>>) -> Option<String> {
    let bytes = state_bytes.as_ref()?;
    let graph: GraphUpdate = rmp_serde::from_slice(bytes).ok()?;

    for node in graph.nodes.values() {
        if let Some(action) = &node.action {
            return Some(action.action_name.clone());
        }
    }
    None
}

fn extract_input_preview(state_bytes: &Option<Vec<u8>>) -> String {
    let Some(bytes) = state_bytes else {
        return "{}".to_string();
    };

    match rmp_serde::from_slice::<GraphUpdate>(bytes) {
        Ok(graph) => {
            let count = graph.nodes.len();
            format!("{{nodes: {count}}}")
        }
        Err(_) => "{}".to_string(),
    }
}

fn format_state_preview(state_bytes: &Option<Vec<u8>>) -> String {
    let Some(bytes) = state_bytes else {
        return "(no state)".to_string();
    };

    match rmp_serde::from_slice::<GraphUpdate>(bytes) {
        Ok(graph) => {
            let summary = serde_json::json!({
                "node_count": graph.nodes.len(),
                "edge_count": graph.edges.len(),
            });
            serde_json::to_string_pretty(&summary).unwrap_or_else(|_| "{}".to_string())
        }
        Err(_) => "(decode error)".to_string(),
    }
}

fn format_result(result_bytes: &Option<Vec<u8>>) -> String {
    let Some(bytes) = result_bytes else {
        return "(pending)".to_string();
    };

    match rmp_serde::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string()),
        Err(_) => "(decode error)".to_string(),
    }
}

fn format_error(error_bytes: &Option<Vec<u8>>) -> Option<String> {
    let bytes = error_bytes.as_ref()?;

    match rmp_serde::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => {
            Some(serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string()))
        }
        Err(_) => Some("(decode error)".to_string()),
    }
}

fn format_node_status(status: &NodeStatus) -> String {
    match status {
        NodeStatus::Queued => "queued".to_string(),
        NodeStatus::Running => "running".to_string(),
        NodeStatus::Completed => "completed".to_string(),
        NodeStatus::Failed => "failed".to_string(),
    }
}
