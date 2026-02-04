//! Postgres backend for persisting runner state and action results.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::base::{
    ActionDone, BackendError, BackendResult, BaseBackend, GraphUpdate, InstanceDone, QueuedInstance,
};

pub const DEFAULT_DSN: &str = "postgresql://rappel:rappel@localhost:5432/rappel_core";

#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    query_counts: Arc<Mutex<HashMap<String, usize>>>,
    batch_size_counts: Arc<Mutex<HashMap<String, HashMap<usize, usize>>>>,
}

impl PostgresBackend {
    pub async fn connect(dsn: &str) -> BackendResult<Self> {
        let pool = PgPool::connect(dsn).await?;
        let backend = Self {
            pool,
            query_counts: Arc::new(Mutex::new(HashMap::new())),
            batch_size_counts: Arc::new(Mutex::new(HashMap::new())),
        };
        backend.ensure_schema().await?;
        Ok(backend)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

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

    pub async fn clear_queue(&self) -> BackendResult<()> {
        Self::count_query(&self.query_counts, "delete:queued_instances_all");
        sqlx::query("DELETE FROM queued_instances")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

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

    async fn ensure_schema(&self) -> BackendResult<()> {
        let statements = [
            r#"
            CREATE TABLE IF NOT EXISTS runner_graph_updates (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                state BYTEA NOT NULL
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS runner_actions_done (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                node_id UUID NOT NULL,
                action_name TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                result BYTEA
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS runner_instances (
                instance_id UUID PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                entry_node UUID NOT NULL,
                state BYTEA,
                result BYTEA,
                error BYTEA
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS runner_instances_done (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                executor_id UUID NOT NULL,
                entry_node UUID NOT NULL,
                result BYTEA,
                error BYTEA
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS queued_instances (
                instance_id UUID PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                payload BYTEA NOT NULL
            )
            "#,
        ];
        let mut tx = self.pool.begin().await?;
        for statement in statements {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
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
        Ok(serde_json::to_vec(value)?)
    }

    fn deserialize<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Result<T, BackendError> {
        Ok(serde_json::from_slice(payload)?)
    }
}

impl BaseBackend for PostgresBackend {
    fn clone_box(&self) -> Box<dyn BaseBackend> {
        Box::new(self.clone())
    }

    fn save_graphs<'a>(&'a self, graphs: &'a [GraphUpdate]) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            if graphs.is_empty() {
                return Ok(());
            }
            Self::count_query(&self.query_counts, "update:runner_instances_state");
            Self::count_batch_size(
                &self.batch_size_counts,
                "update:runner_instances_state",
                graphs.len(),
            );
            let mut tx = self.pool.begin().await?;
            for graph in graphs {
                let payload = Self::serialize(graph)?;
                sqlx::query("UPDATE runner_instances SET state = $1 WHERE instance_id = $2")
                    .bind(payload)
                    .bind(graph.instance_id)
                    .execute(&mut *tx)
                    .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [ActionDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
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
        })
    }

    fn get_queued_instances<'a>(
        &'a self,
        size: usize,
    ) -> BoxFuture<'a, BackendResult<Vec<QueuedInstance>>> {
        Box::pin(async move {
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
        })
    }

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [InstanceDone],
    ) -> BoxFuture<'a, BackendResult<()>> {
        Box::pin(async move {
            if instances.is_empty() {
                return Ok(());
            }
            Self::count_query(&self.query_counts, "update:runner_instances_result");
            Self::count_batch_size(
                &self.batch_size_counts,
                "update:runner_instances_result",
                instances.len(),
            );
            let mut tx = self.pool.begin().await?;
            for instance in instances {
                let result = match &instance.result {
                    Some(value) => Some(Self::serialize(value)?),
                    None => None,
                };
                let error = match &instance.error {
                    Some(value) => Some(Self::serialize(value)?),
                    None => None,
                };
                sqlx::query(
                    "UPDATE runner_instances SET result = $1, error = $2 WHERE instance_id = $3",
                )
                .bind(result)
                .bind(error)
                .bind(instance.executor_id)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
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
