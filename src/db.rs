use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use prost::Message;
use serde_json::json;
use sqlx::{
    FromRow, PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};

use crate::messages::proto::{
    WorkflowDagDefinition, WorkflowDagNode, WorkflowNodeContext, WorkflowNodeDispatch,
};

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

async fn load_instance_node_state(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: i64,
) -> Result<(HashSet<String>, HashSet<String>)> {
    let rows = sqlx::query(
        r#"
        SELECT workflow_node_id, status
        FROM daemon_action_ledger
        WHERE instance_id = $1
        "#,
    )
    .bind(instance_id)
    .map(|row: PgRow| (row.get::<Option<String>, _>(0), row.get::<String, _>(1)))
    .fetch_all(tx.as_mut())
    .await?;
    let mut known_nodes = HashSet::new();
    let mut completed_nodes = HashSet::new();
    for (node_id_opt, status) in rows {
        if let Some(node_id) = node_id_opt {
            if status.eq_ignore_ascii_case("completed") {
                completed_nodes.insert(node_id.clone());
            }
            known_nodes.insert(node_id);
        }
    }
    Ok((known_nodes, completed_nodes))
}

fn determine_unlocked_nodes(
    dag: &WorkflowDagDefinition,
    known_nodes: &HashSet<String>,
    completed_nodes: &HashSet<String>,
    concurrent: bool,
) -> Vec<WorkflowDagNode> {
    dag.nodes
        .iter()
        .filter(|node| {
            if known_nodes.contains(&node.id) {
                return false;
            }
            required_dependencies(node, concurrent)
                .into_iter()
                .all(|dep| completed_nodes.contains(dep))
        })
        .cloned()
        .collect()
}

async fn build_variable_context(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: i64,
    dag_nodes: &HashMap<String, WorkflowDagNode>,
) -> Result<HashMap<String, Vec<u8>>> {
    let rows = sqlx::query(
        r#"
        SELECT workflow_node_id, result_payload
        FROM daemon_action_ledger
        WHERE instance_id = $1 AND status = 'completed' AND workflow_node_id IS NOT NULL AND result_payload IS NOT NULL
        "#,
    )
    .bind(instance_id)
    .map(|row: PgRow| (row.get::<Option<String>, _>(0), row.get::<Option<Vec<u8>>, _>(1)))
    .fetch_all(tx.as_mut())
    .await?;
    let mut context = HashMap::new();
    for (node_id_opt, payload_opt) in rows {
        let Some(node_id) = node_id_opt else {
            continue;
        };
        let Some(payload) = payload_opt else {
            continue;
        };
        if let Some(node) = dag_nodes.get(&node_id) {
            for variable in &node.produces {
                context.insert(variable.clone(), payload.clone());
            }
        }
    }
    Ok(context)
}

fn required_dependencies(node: &WorkflowDagNode, concurrent: bool) -> Vec<&str> {
    let mut deps: Vec<&str> = node.depends_on.iter().map(|s| s.as_str()).collect();
    if !concurrent {
        deps.extend(node.wait_for_sync.iter().map(|s| s.as_str()));
    }
    deps
}

fn build_workflow_action_payload(
    node: &WorkflowDagNode,
    workflow_input: &[u8],
    context: &HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut dispatch = WorkflowNodeDispatch {
        node: Some(node.clone()),
        workflow_input: workflow_input.to_vec(),
        context: Vec::new(),
    };
    for (variable, payload) in context {
        dispatch.context.push(WorkflowNodeContext {
            variable: variable.clone(),
            payload: payload.clone(),
        });
    }
    let dispatch_bytes = dispatch.encode_to_vec();
    let encoded = general_purpose::STANDARD.encode(dispatch_bytes);
    let payload = json!({
        "module": "carabiner_worker.workflow_runtime",
        "action": "workflow.execute_node",
        "kwargs": {
            "dispatch_b64": {
                "kind": "primitive",
                "value": encoded,
            }
        }
    });
    Ok(serde_json::to_vec(&payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_node(id: &str, depends_on: &[&str], wait_for_sync: &[&str]) -> WorkflowDagNode {
        WorkflowDagNode {
            id: id.to_string(),
            action: format!("action_{id}"),
            kwargs: Default::default(),
            depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
            wait_for_sync: wait_for_sync.iter().map(|s| s.to_string()).collect(),
            module: String::new(),
            produces: Vec::new(),
        }
    }

    #[test]
    fn sequential_workflows_respect_wait_for_sync() {
        let dag = WorkflowDagDefinition {
            concurrent: false,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &[], &["node_0"]),
            ],
        };
        let mut known = HashSet::new();
        let mut completed = HashSet::new();
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, false);
        assert_eq!(
            unlocked.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            vec!["node_0"]
        );

        known.insert("node_0".to_string());
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, false);
        assert!(unlocked.is_empty());

        completed.insert("node_0".to_string());
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, false);
        assert_eq!(
            unlocked.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            vec!["node_1"]
        );
    }

    #[test]
    fn concurrent_workflows_ignore_wait_for_sync() {
        let dag = WorkflowDagDefinition {
            concurrent: true,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &[], &["node_0"]),
            ],
        };
        let known = HashSet::new();
        let completed = HashSet::new();
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, true);
        let actions: Vec<&str> = unlocked.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(actions, vec!["node_0", "node_1"]);
    }

    #[test]
    fn dependencies_gate_unlocking() {
        let dag = WorkflowDagDefinition {
            concurrent: true,
            nodes: vec![
                build_node("node_0", &[], &[]),
                build_node("node_1", &["node_0"], &[]),
            ],
        };
        let mut known = HashSet::new();
        let mut completed = HashSet::new();
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, true);
        assert_eq!(
            unlocked.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            vec!["node_0"]
        );

        known.insert("node_0".to_string());
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, true);
        assert!(unlocked.is_empty());

        completed.insert("node_0".to_string());
        let unlocked = determine_unlocked_nodes(&dag, &known, &completed, true);
        assert_eq!(
            unlocked.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
            vec!["node_1"]
        );
    }
}

#[derive(Debug, Clone)]
pub struct CompletionRecord {
    pub action_id: i64,
    pub success: bool,
    pub delivery_id: u64,
    pub result_payload: Vec<u8>,
}

#[derive(Debug, Clone, FromRow)]
pub struct LedgerAction {
    pub id: i64,
    pub instance_id: i64,
    pub partition_id: i32,
    pub action_seq: i32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, FromRow)]
struct WorkflowInstanceRow {
    pub id: i64,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<i64>,
    pub next_action_seq: i32,
    pub input_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
struct WorkflowVersionRow {
    pub concurrent: bool,
    pub dag_proto: Vec<u8>,
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .with_context(|| "failed to connect to postgres")?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .with_context(|| "failed to run migrations")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn reset_partition(&self, partition_id: i32) -> Result<()> {
        let span = tracing::info_span!("db.reset_partition", partition_id);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM daemon_action_ledger WHERE partition_id = $1")
            .bind(partition_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM workflow_instances WHERE partition_id = $1")
            .bind(partition_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn seed_actions(
        &self,
        partition_id: i32,
        action_count: usize,
        payload: &[u8],
    ) -> Result<()> {
        let span = tracing::info_span!("db.seed_actions", partition_id, action_count);
        let _guard = span.enter();
        let mut tx = self.pool.begin().await?;
        let instance_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO workflow_instances (partition_id, workflow_name, next_action_seq)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(partition_id)
        .bind("benchmark")
        .bind(action_count as i32)
        .fetch_one(&mut *tx)
        .await?;

        for seq in 0..action_count {
            sqlx::query(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id,
                    partition_id,
                    action_seq,
                    status,
                    payload
                ) VALUES ($1, $2, $3, 'queued', $4)
                "#,
            )
            .bind(instance_id)
            .bind(partition_id)
            .bind(seq as i32)
            .bind(payload)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn create_workflow_instance(
        &self,
        partition_id: i32,
        workflow_name: &str,
        workflow_version_id: i64,
        input_payload: Option<&[u8]>,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let instance_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO workflow_instances (
                partition_id,
                workflow_name,
                workflow_version_id,
                input_payload
            )
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(partition_id)
        .bind(workflow_name)
        .bind(workflow_version_id)
        .bind(input_payload)
        .fetch_one(&mut *tx)
        .await?;
        self.schedule_workflow_instance_tx(&mut tx, instance_id)
            .await?;
        tx.commit().await?;
        Ok(instance_id)
    }

    pub async fn schedule_workflow_instance(&self, instance_id: i64) -> Result<usize> {
        let mut tx = self.pool.begin().await?;
        let count = self
            .schedule_workflow_instance_tx(&mut tx, instance_id)
            .await?;
        tx.commit().await?;
        Ok(count)
    }

    pub async fn dispatch_actions(
        &self,
        partition_id: i32,
        limit: i64,
    ) -> Result<Vec<LedgerAction>> {
        let span = tracing::info_span!("db.dispatch_actions", partition_id, limit);
        let _guard = span.enter();
        let records = sqlx::query_as::<_, LedgerAction>(
            r#"
            WITH next_actions AS (
                SELECT id
                FROM daemon_action_ledger
                WHERE partition_id = $1 AND status = 'queued'
                ORDER BY action_seq
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'dispatched', dispatched_at = NOW()
            FROM next_actions
            WHERE dal.id = next_actions.id
            RETURNING dal.id, dal.instance_id, dal.partition_id, dal.action_seq, dal.payload
            "#,
        )
        .bind(partition_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(records)
    }

    pub async fn mark_actions_batch(&self, records: &[CompletionRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let span = tracing::debug_span!("db.mark_actions_batch", count = records.len());
        let _guard = span.enter();
        let ids: Vec<i64> = records.iter().map(|r| r.action_id).collect();
        let successes: Vec<bool> = records.iter().map(|r| r.success).collect();
        let deliveries: Vec<i64> = records.iter().map(|r| r.delivery_id as i64).collect();
        let payloads: Vec<Vec<u8>> = records.iter().map(|r| r.result_payload.clone()).collect();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            r#"
            WITH data AS (
                SELECT *
                FROM UNNEST($1::BIGINT[], $2::BOOL[], $3::BIGINT[], $4::BYTEA[])
                    AS t(action_id, success, delivery_id, result_payload)
            )
            UPDATE daemon_action_ledger AS dal
            SET status = 'completed',
                success = data.success,
                completed_at = NOW(),
                acked_at = COALESCE(dal.acked_at, NOW()),
                delivery_id = data.delivery_id,
                result_payload = data.result_payload
            FROM data
            WHERE dal.id = data.action_id
            RETURNING dal.instance_id, dal.workflow_node_id
            "#,
        )
        .bind(&ids)
        .bind(&successes)
        .bind(&deliveries)
        .bind(&payloads)
        .map(|row: PgRow| (row.get::<i64, _>(0), row.get::<Option<String>, _>(1)))
        .fetch_all(&mut *tx)
        .await?;

        let mut workflow_instances = HashSet::new();
        for row in updated {
            if row.1.is_some() {
                workflow_instances.insert(row.0);
            }
        }

        for instance_id in workflow_instances {
            let inserted = self
                .schedule_workflow_instance_tx(&mut tx, instance_id)
                .await?;
            if inserted > 0 {
                tracing::debug!(
                    instance_id,
                    inserted,
                    "workflow actions unlocked after completion"
                );
            }
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_workflow_version(
        &self,
        workflow_name: &str,
        dag_hash: &str,
        dag_bytes: &[u8],
        concurrent: bool,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        if let Some(existing_id) = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM workflow_versions WHERE workflow_name = $1 AND dag_hash = $2",
        )
        .bind(workflow_name)
        .bind(dag_hash)
        .fetch_optional(tx.as_mut())
        .await?
        {
            tx.commit().await?;
            return Ok(existing_id);
        }

        let id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO workflow_versions (workflow_name, dag_hash, dag_proto, concurrent)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(workflow_name)
        .bind(dag_hash)
        .bind(dag_bytes)
        .bind(concurrent)
        .fetch_one(tx.as_mut())
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    async fn schedule_workflow_instance_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        instance_id: i64,
    ) -> Result<usize> {
        let instance = sqlx::query_as::<_, WorkflowInstanceRow>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id, next_action_seq, input_payload
            FROM workflow_instances
            WHERE id = $1
            FOR UPDATE
            "#,
        )
        .bind(instance_id)
        .fetch_optional(tx.as_mut())
        .await?;
        let Some(instance) = instance else {
            tracing::warn!(instance_id, "workflow instance not found during scheduling");
            return Ok(0);
        };
        let Some(version_id) = instance.workflow_version_id else {
            tracing::debug!(
                instance_id = instance.id,
                "workflow instance missing version; skipping scheduling"
            );
            return Ok(0);
        };
        let version = sqlx::query_as::<_, WorkflowVersionRow>(
            r#"
            SELECT concurrent, dag_proto
            FROM workflow_versions
            WHERE id = $1
            "#,
        )
        .bind(version_id)
        .fetch_one(tx.as_mut())
        .await?;
        let dag = WorkflowDagDefinition::decode(version.dag_proto.as_slice())
            .context("failed to decode workflow dag for scheduling")?;
        let mut node_map = HashMap::new();
        for node in &dag.nodes {
            node_map.insert(node.id.clone(), node.clone());
        }
        let (known_nodes, completed_nodes) = load_instance_node_state(tx, instance.id).await?;
        let unlocked =
            determine_unlocked_nodes(&dag, &known_nodes, &completed_nodes, version.concurrent);
        if unlocked.is_empty() {
            return Ok(0);
        }
        let workflow_input = instance.input_payload.clone().unwrap_or_default();
        let context_values = build_variable_context(tx, instance.id, &node_map).await?;
        let mut next_sequence = instance.next_action_seq;
        let mut inserted = 0usize;
        for node in unlocked {
            let node_id = node.id.clone();
            let payload = build_workflow_action_payload(&node, &workflow_input, &context_values)?;
            let _action_id: i64 = sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO daemon_action_ledger (
                    instance_id,
                    partition_id,
                    action_seq,
                    status,
                    payload,
                    workflow_node_id
                ) VALUES ($1, $2, $3, 'queued', $4, $5)
                RETURNING id
                "#,
            )
            .bind(instance.id)
            .bind(instance.partition_id)
            .bind(next_sequence)
            .bind(payload)
            .bind(node_id)
            .fetch_one(tx.as_mut())
            .await?;
            inserted += 1;
            next_sequence += 1;
        }
        if next_sequence != instance.next_action_seq {
            sqlx::query("UPDATE workflow_instances SET next_action_seq = $2 WHERE id = $1")
                .bind(instance.id)
                .bind(next_sequence)
                .execute(tx.as_mut())
                .await?;
        }
        tracing::debug!(
            instance_id = instance.id,
            workflow = instance.workflow_name,
            inserted,
            "queued workflow actions"
        );
        Ok(inserted)
    }
}
