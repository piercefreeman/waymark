use std::collections::HashMap;

use chrono::Utc;
use sqlx::{Postgres, QueryBuilder, Row};
use tonic::async_trait;
use uuid::Uuid;

use super::PostgresBackend;
use crate::backends::base::{
    ActionDone, BackendError, BackendResult, CoreBackend, GraphUpdate, InstanceDone,
    InstanceLockStatus, LockClaim, QueuedInstance, QueuedInstanceBatch, WorkerStatusBackend,
    WorkerStatusUpdate,
};
use crate::observability::obs;
use crate::rappel_core::runner::state::RunnerState;

impl PostgresBackend {
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
            SET lock_uuid = NULL, lock_expires_at = NULL
            FROM expired
            WHERE qi.instance_id = expired.instance_id
            RETURNING qi.instance_id
            "#,
        )
        .bind(now)
        .bind(size as i64)
        .fetch_all(&mut *tx)
        .await?;
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
            ") AS v(instance_id, state)
             JOIN queued_instances qi ON qi.instance_id = v.instance_id
             WHERE ri.instance_id = v.instance_id
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
            ") AS v(instance_id, scheduled_at)
             WHERE qi.instance_id = v.instance_id
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::{Duration, Utc};
    use serial_test::serial;
    use sqlx::{PgPool, Row};
    use uuid::Uuid;

    use super::*;
    use crate::backends::{CoreBackend, WorkerStatusBackend};
    use crate::rappel_core::dag::EdgeType;
    use crate::rappel_core::runner::state::{ActionCallSpec, ExecutionNode, NodeStatus};
    use crate::test_support::postgres_setup;

    async fn setup_backend() -> PostgresBackend {
        let pool = postgres_setup().await;
        reset_database(&pool).await;
        PostgresBackend::new(pool)
    }

    async fn reset_database(pool: &PgPool) {
        sqlx::query(
            r#"
            TRUNCATE runner_graph_updates,
                     runner_actions_done,
                     runner_instances_done,
                     queued_instances,
                     runner_instances,
                     workflow_versions,
                     workflow_schedules,
                     worker_status
            RESTART IDENTITY CASCADE
            "#,
        )
        .execute(pool)
        .await
        .expect("truncate postgres tables");
    }

    fn sample_runner_state() -> RunnerState {
        RunnerState::new(None, None, None, false)
    }

    fn sample_queued_instance(instance_id: Uuid, entry_node: Uuid) -> QueuedInstance {
        QueuedInstance {
            workflow_version_id: Uuid::new_v4(),
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
            scheduled_at: Some(Utc::now() + Duration::seconds(15)),
        }
    }

    fn sample_lock_claim() -> LockClaim {
        LockClaim {
            lock_uuid: Uuid::new_v4(),
            lock_expires_at: Utc::now() + Duration::seconds(30),
        }
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
    }

    #[serial(postgres)]
    #[tokio::test]
    async fn core_get_queued_instances_happy_path() {
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
                crate::rappel_core::runner::state::ExecutionEdge {
                    source: execution_id,
                    target: execution_id,
                    edge_type: EdgeType::StateMachine,
                },
            ]),
        };

        let locks =
            CoreBackend::save_graphs(&backend, claim.lock_uuid, std::slice::from_ref(&graph))
                .await
                .expect("save graphs");
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].instance_id, instance_id);
        assert_eq!(locks[0].lock_uuid, Some(claim.lock_uuid));

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
    async fn core_save_actions_done_happy_path() {
        let backend = setup_backend().await;
        let execution_id = Uuid::new_v4();
        CoreBackend::save_actions_done(
            &backend,
            &[ActionDone {
                execution_id,
                attempt: 1,
                result: serde_json::json!({"ok": true}),
            }],
        )
        .await
        .expect("save actions done");

        let row = sqlx::query(
            "SELECT execution_id, attempt, result FROM runner_actions_done WHERE execution_id = $1",
        )
        .bind(execution_id)
        .fetch_one(backend.pool())
        .await
        .expect("action row");

        assert_eq!(row.get::<Uuid, _>("execution_id"), execution_id);
        assert_eq!(row.get::<i32, _>("attempt"), 1);
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
        assert_eq!(refreshed[0].lock_expires_at, Some(refreshed_expiry));
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

        let live_lock = lock_rows.get(&live_id).expect("live lock row");
        assert_eq!(live_lock.0, Some(claim.lock_uuid));
        assert_eq!(live_lock.1, Some(live_at));
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
