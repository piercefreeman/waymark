//! Postgres backend for persisting runner state and action results.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::db;
use crate::observability::obs;

use super::base::{
    ActionDone, BackendError, BackendResult, BaseBackend, GraphUpdate, InstanceDone,
    QueuedInstance, WorkerStatusBackend, WorkerStatusUpdate,
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
            queued_payloads.push((instance.instance_id, Self::serialize(instance)?));
            let graph = build_graph_update(instance, instance.instance_id);
            runner_payloads.push((
                instance.instance_id,
                instance.entry_node,
                Self::serialize(&graph)?,
            ));
        }

        let mut queued_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("INSERT INTO queued_instances (instance_id, payload) ");
        queued_builder.push_values(queued_payloads.iter(), |mut builder, (id, payload)| {
            builder.push_bind(*id).push_bind(payload.as_slice());
        });

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
    async fn save_graphs_impl(&self, graphs: &[GraphUpdate]) -> BackendResult<()> {
        if graphs.is_empty() {
            return Ok(());
        }
        Self::count_query(&self.query_counts, "update:runner_instances_state");
        Self::count_batch_size(
            &self.batch_size_counts,
            "update:runner_instances_state",
            graphs.len(),
        );
        let mut payloads = Vec::with_capacity(graphs.len());
        for graph in graphs {
            payloads.push((graph.instance_id, Self::serialize(graph)?));
        }
        let mut builder: QueryBuilder<Postgres> =
            QueryBuilder::new("UPDATE runner_instances AS ri SET state = v.state FROM (");
        builder.push_values(payloads.iter(), |mut b, (instance_id, payload)| {
            b.push_bind(*instance_id).push_bind(payload.as_slice());
        });
        builder.push(") AS v(instance_id, state) WHERE ri.instance_id = v.instance_id");
        builder.build().execute(&self.pool).await?;
        Ok(())
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
                action.node_id,
                action.action_name.clone(),
                action.attempt,
                Self::serialize(&action.result)?,
            ));
        }
        let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO runner_actions_done (node_id, action_name, attempt, result) ",
        );
        builder.push_values(
            payloads.iter(),
            |mut b, (node_id, name, attempt, payload)| {
                b.push_bind(*node_id)
                    .push_bind(name)
                    .push_bind(*attempt)
                    .push_bind(payload.as_slice());
            },
        );
        builder.build().execute(&self.pool).await?;
        Ok(())
    }

    #[obs]
    async fn get_queued_instances_impl(&self, size: usize) -> BackendResult<Vec<QueuedInstance>> {
        if size == 0 {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await?;
        Self::count_query(&self.query_counts, "select:queued_instances");
        let rows = sqlx::query(
            "SELECT instance_id, payload FROM queued_instances ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED",
        )
        .bind(size as i64)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            tx.commit().await?;
            return Ok(Vec::new());
        }
        Self::count_batch_size(
            &self.batch_size_counts,
            "select:queued_instances",
            rows.len(),
        );
        let ids: Vec<Uuid> = rows.iter().map(|row| row.get::<Uuid, _>(0)).collect();
        Self::count_query(&self.query_counts, "delete:queued_instances_by_id");
        sqlx::query("DELETE FROM queued_instances WHERE instance_id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut instances = Vec::new();
        for row in rows {
            let instance_id: Uuid = row.get(0);
            let payload: Vec<u8> = row.get(1);
            let mut instance: QueuedInstance = Self::deserialize(&payload)?;
            instance.instance_id = instance_id;
            instances.push(instance);
        }
        Ok(instances)
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
        Ok(())
    }
}

impl BaseBackend for PostgresBackend {
    fn clone_box(&self) -> Box<dyn BaseBackend> {
        Box::new(self.clone())
    }

    fn save_graphs<'a>(&'a self, graphs: &'a [GraphUpdate]) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(self.save_graphs_impl(graphs))
    }

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(self.save_actions_done_impl(actions))
    }

    fn get_queued_instances<'a>(
        &'a self,
        size: usize,
    ) -> BoxFuture<'a, BackendResult<Vec<QueuedInstance>>> {
        Box::pin(self.get_queued_instances_impl(size))
    }

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(self.save_instances_done_impl(instances))
    }
}

impl WorkerStatusBackend for PostgresBackend {
    fn upsert_worker_status<'a>(
        &'a self,
        status: &'a WorkerStatusUpdate,
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move { PostgresBackend::upsert_worker_status(self, status).await })
    }
}

fn build_graph_update(instance: &QueuedInstance, instance_id: Uuid) -> GraphUpdate {
    if let Some(state) = &instance.state {
        return GraphUpdate {
            instance_id,
            nodes: state.nodes.clone(),
            edges: state.edges.clone(),
        };
    }
    let nodes = instance
        .nodes
        .clone()
        .expect("queued instance missing nodes");
    let edges = instance
        .edges
        .clone()
        .expect("queued instance missing edges");
    GraphUpdate {
        instance_id,
        nodes,
        edges,
    }
}
