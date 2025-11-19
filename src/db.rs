use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::json;
use sqlx::{
    FromRow, PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};

use crate::{
    LedgerActionId, WorkflowInstanceId, WorkflowVersionId,
    dag_state::{DagStateMachine, InstanceDagState},
    messages::proto::{
        WorkflowDagDefinition, WorkflowDagNode, WorkflowNodeContext, WorkflowNodeDispatch,
    },
};

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

async fn store_instance_result_if_complete(
    tx: &mut Transaction<'_, Postgres>,
    instance: &WorkflowInstanceRow,
    dag: &WorkflowDagDefinition,
    context_values: &HashMap<String, Vec<(String, Vec<u8>)>>,
    completed_count: usize,
) -> Result<()> {
    if dag.nodes.len() != completed_count {
        return Ok(());
    }
    if dag.return_variable.is_empty() {
        return Ok(());
    }
    if instance.result_payload.is_some() {
        return Ok(());
    }
    let mut payload_opt: Option<Vec<u8>> = None;
    for entries in context_values.values() {
        for (variable, payload) in entries {
            if variable == &dag.return_variable {
                payload_opt = Some(payload.clone());
                break;
            }
        }
        if payload_opt.is_some() {
            break;
        }
    }
    let Some(payload) = payload_opt else {
        return Ok(());
    };
    sqlx::query("UPDATE workflow_instances SET result_payload = $2 WHERE id = $1")
        .bind(instance.id)
        .bind(payload)
        .execute(tx.as_mut())
        .await?;
    Ok(())
}

async fn load_instance_node_state(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
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

async fn build_variable_context(
    tx: &mut Transaction<'_, Postgres>,
    instance_id: WorkflowInstanceId,
    dag_nodes: &HashMap<String, WorkflowDagNode>,
) -> Result<HashMap<String, Vec<(String, Vec<u8>)>>> {
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
    let mut context: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
    for (node_id_opt, payload_opt) in rows {
        let Some(node_id) = node_id_opt else {
            continue;
        };
        let Some(payload) = payload_opt else {
            continue;
        };
        if let Some(node) = dag_nodes.get(&node_id) {
            let entries = context.entry(node_id.clone()).or_default();
            if node.produces.is_empty() {
                entries.push((String::new(), payload.clone()));
            } else {
                for variable in &node.produces {
                    entries.push((variable.clone(), payload.clone()));
                }
            }
        }
    }
    Ok(context)
}

fn build_workflow_action_payload(
    node: &WorkflowDagNode,
    workflow_input: &[u8],
    context: &HashMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<Vec<u8>> {
    let mut dispatch = WorkflowNodeDispatch {
        node: Some(node.clone()),
        workflow_input: workflow_input.to_vec(),
        context: Vec::new(),
    };
    let mut required: Vec<&str> = node.depends_on.iter().map(|s| s.as_str()).collect();
    required.extend(node.wait_for_sync.iter().map(|s| s.as_str()));
    let mut seen: HashSet<&str> = HashSet::new();
    for dep in required {
        if !seen.insert(dep) {
            continue;
        }
        if let Some(entries) = context.get(dep) {
            for (variable, payload) in entries {
                dispatch.context.push(WorkflowNodeContext {
                    variable: variable.clone(),
                    payload: payload.clone(),
                    workflow_node_id: dep.to_string(),
                });
            }
        }
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

#[derive(Debug, Clone)]
pub struct CompletionRecord {
    pub action_id: LedgerActionId,
    pub success: bool,
    pub delivery_id: u64,
    pub result_payload: Vec<u8>,
}

#[derive(Debug, Clone, FromRow)]
pub struct LedgerAction {
    pub id: LedgerActionId,
    pub instance_id: WorkflowInstanceId,
    pub partition_id: i32,
    pub action_seq: i32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, FromRow)]
struct WorkflowInstanceRow {
    pub id: WorkflowInstanceId,
    pub partition_id: i32,
    pub workflow_name: String,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub next_action_seq: i32,
    pub input_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone, FromRow)]
struct WorkflowVersionRow {
    pub concurrent: bool,
    pub dag_proto: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct WorkflowVersionSummary {
    pub id: WorkflowVersionId,
    pub workflow_name: String,
    pub dag_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WorkflowVersionDetail {
    pub id: WorkflowVersionId,
    pub workflow_name: String,
    pub dag_hash: String,
    pub concurrent: bool,
    pub created_at: DateTime<Utc>,
    pub dag: WorkflowDagDefinition,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceSummary {
    pub id: WorkflowInstanceId,
    pub created_at: DateTime<Utc>,
    pub completed_actions: i64,
    pub total_actions: i64,
    pub has_result: bool,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceDetail {
    pub instance: WorkflowInstanceRecord,
    pub actions: Vec<WorkflowInstanceActionDetail>,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceRecord {
    pub id: WorkflowInstanceId,
    pub workflow_version_id: Option<WorkflowVersionId>,
    pub workflow_name: String,
    pub created_at: DateTime<Utc>,
    pub input_payload: Option<Vec<u8>>,
    pub result_payload: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct WorkflowInstanceActionDetail {
    pub action_seq: i32,
    pub workflow_node_id: Option<String>,
    pub status: String,
    pub success: Option<bool>,
    pub payload: Vec<u8>,
    pub result_payload: Option<Vec<u8>>,
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

    pub async fn list_workflow_versions(&self) -> Result<Vec<WorkflowVersionSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_name, dag_hash, created_at
            FROM workflow_versions
            ORDER BY created_at DESC
            "#,
        )
        .map(|row: PgRow| WorkflowVersionSummary {
            id: row.get("id"),
            workflow_name: row.get("workflow_name"),
            dag_hash: row.get("dag_hash"),
            created_at: row.get("created_at"),
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn load_workflow_version(
        &self,
        version_id: WorkflowVersionId,
    ) -> Result<Option<WorkflowVersionDetail>> {
        let row = sqlx::query(
            r#"
            SELECT id, workflow_name, dag_hash, dag_proto, concurrent, created_at
            FROM workflow_versions
            WHERE id = $1
            "#,
        )
        .bind(version_id)
        .map(|row: PgRow| {
            (
                row.get::<WorkflowVersionId, _>("id"),
                row.get::<String, _>("workflow_name"),
                row.get::<String, _>("dag_hash"),
                row.get::<Vec<u8>, _>("dag_proto"),
                row.get::<bool, _>("concurrent"),
                row.get::<DateTime<Utc>, _>("created_at"),
            )
        })
        .fetch_optional(&self.pool)
        .await?;
        let Some((id, workflow_name, dag_hash, dag_proto, concurrent, created_at)) = row else {
            return Ok(None);
        };
        let dag = WorkflowDagDefinition::decode(dag_proto.as_slice())
            .context("failed to decode workflow dag")?;
        Ok(Some(WorkflowVersionDetail {
            id,
            workflow_name,
            dag_hash,
            concurrent,
            created_at,
            dag,
        }))
    }

    pub async fn recent_instances_for_version(
        &self,
        version_id: WorkflowVersionId,
        limit: i64,
    ) -> Result<Vec<WorkflowInstanceSummary>> {
        let rows = sqlx::query(
            r#"
            SELECT wi.id,
                   wi.created_at,
                   wi.result_payload IS NOT NULL AS has_result,
                   COUNT(dal.id) AS total_actions,
                   COUNT(*) FILTER (WHERE dal.status = 'completed') AS completed_actions
            FROM workflow_instances wi
            LEFT JOIN daemon_action_ledger dal ON dal.instance_id = wi.id
            WHERE wi.workflow_version_id = $1
            GROUP BY wi.id
            ORDER BY wi.created_at DESC
            LIMIT $2
            "#,
        )
        .bind(version_id)
        .bind(limit.max(1))
        .map(|row: PgRow| WorkflowInstanceSummary {
            id: row.get("id"),
            created_at: row.get("created_at"),
            completed_actions: row.get::<i64, _>("completed_actions"),
            total_actions: row.get::<i64, _>("total_actions"),
            has_result: row.get::<bool, _>("has_result"),
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn load_instance_with_actions(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> Result<Option<WorkflowInstanceDetail>> {
        let row = sqlx::query(
            r#"
            SELECT id, workflow_name, workflow_version_id, created_at, input_payload, result_payload
            FROM workflow_instances
            WHERE id = $1
            "#,
        )
        .bind(instance_id)
        .map(|row: PgRow| WorkflowInstanceRecord {
            id: row.get("id"),
            workflow_name: row.get("workflow_name"),
            workflow_version_id: row.get("workflow_version_id"),
            created_at: row.get("created_at"),
            input_payload: row.get("input_payload"),
            result_payload: row.get("result_payload"),
        })
        .fetch_optional(&self.pool)
        .await?;
        let Some(instance) = row else {
            return Ok(None);
        };
        let actions = sqlx::query(
            r#"
            SELECT action_seq, workflow_node_id, status, success, payload, result_payload
            FROM daemon_action_ledger
            WHERE instance_id = $1
            ORDER BY action_seq
            "#,
        )
        .bind(instance_id)
        .map(|row: PgRow| WorkflowInstanceActionDetail {
            action_seq: row.get("action_seq"),
            workflow_node_id: row.get("workflow_node_id"),
            status: row.get("status"),
            success: row.get("success"),
            payload: row.get::<Vec<u8>, _>("payload"),
            result_payload: row.get("result_payload"),
        })
        .fetch_all(&self.pool)
        .await?;
        Ok(Some(WorkflowInstanceDetail { instance, actions }))
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
        let instance_id: WorkflowInstanceId = sqlx::query_scalar(
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
        workflow_version_id: WorkflowVersionId,
        input_payload: Option<&[u8]>,
    ) -> Result<WorkflowInstanceId> {
        let mut tx = self.pool.begin().await?;
        let instance_id = sqlx::query_scalar::<_, WorkflowInstanceId>(
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

    pub async fn schedule_workflow_instance(
        &self,
        instance_id: WorkflowInstanceId,
    ) -> Result<usize> {
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
        let ids: Vec<LedgerActionId> = records.iter().map(|r| r.action_id).collect();
        let successes: Vec<bool> = records.iter().map(|r| r.success).collect();
        let deliveries: Vec<i64> = records.iter().map(|r| r.delivery_id as i64).collect();
        let payloads: Vec<Vec<u8>> = records.iter().map(|r| r.result_payload.clone()).collect();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            r#"
            WITH data AS (
                SELECT *
                FROM UNNEST($1::UUID[], $2::BOOL[], $3::BIGINT[], $4::BYTEA[])
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
        .map(|row: PgRow| {
            (
                row.get::<WorkflowInstanceId, _>(0),
                row.get::<Option<String>, _>(1),
            )
        })
        .fetch_all(&mut *tx)
        .await?;

        let mut workflow_instances: HashSet<WorkflowInstanceId> = HashSet::new();
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
                    instance_id = %instance_id,
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
    ) -> Result<WorkflowVersionId> {
        let mut tx = self.pool.begin().await?;
        if let Some(existing_id) = sqlx::query_scalar::<_, WorkflowVersionId>(
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

        let id = sqlx::query_scalar::<_, WorkflowVersionId>(
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
        instance_id: WorkflowInstanceId,
    ) -> Result<usize> {
        let instance = sqlx::query_as::<_, WorkflowInstanceRow>(
            r#"
            SELECT id, partition_id, workflow_name, workflow_version_id, next_action_seq, input_payload, result_payload
            FROM workflow_instances
            WHERE id = $1
            FOR UPDATE
            "#,
        )
        .bind(instance_id)
        .fetch_optional(tx.as_mut())
        .await?;
        let Some(instance) = instance else {
            tracing::warn!(instance_id = %instance_id, "workflow instance not found during scheduling");
            return Ok(0);
        };
        let Some(version_id) = instance.workflow_version_id else {
            tracing::debug!(
                instance_id = %instance.id,
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
        let dag_state = InstanceDagState::new(known_nodes, completed_nodes);
        let dag_machine = DagStateMachine::new(&dag, version.concurrent);
        let context_values = build_variable_context(tx, instance.id, &node_map).await?;
        let completed_count = dag_state.completed().len();
        let unlocked = dag_machine.ready_nodes(&dag_state);
        if unlocked.is_empty() {
            store_instance_result_if_complete(
                tx,
                &instance,
                &dag,
                &context_values,
                completed_count,
            )
            .await?;
            return Ok(0);
        }
        let workflow_input = instance.input_payload.clone().unwrap_or_default();
        let mut next_sequence = instance.next_action_seq;
        let mut inserted = 0usize;
        for node in unlocked {
            let node_id = node.id.clone();
            let payload = build_workflow_action_payload(&node, &workflow_input, &context_values)?;
            let _action_id: LedgerActionId = sqlx::query_scalar::<_, LedgerActionId>(
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
            instance_id = %instance.id,
            workflow = instance.workflow_name,
            inserted,
            "queued workflow actions"
        );
        Ok(inserted)
    }
}
