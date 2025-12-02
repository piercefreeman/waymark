//! Persistence layer for workflow state.
//!
//! This module provides storage for:
//! - Workflow DAG definitions
//! - Instance state (eval_context, node_states, loop_indices)
//! - Action queue for worker dispatch
//!
//! The design is intentionally simple:
//! - State is JSON-serialized for flexibility
//! - Single source of truth in eval_context (no separate accumulator tracking)
//! - Push-based scheduling via ready queue

use crate::dag::Dag;
use crate::ir_to_dag::convert_workflow;
use crate::messages::proto::{
    NodeDispatch, WorkflowArguments, WorkflowRegistration,
};
use crate::scheduler::{InstanceState, Scheduler, SchedulerAction};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use std::collections::HashMap;
use uuid::Uuid;

/// Workflow version stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredWorkflow {
    pub id: Uuid,
    pub name: String,
    pub dag_json: String,
    pub concurrent: bool,
}

/// Workflow instance stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredInstance {
    pub id: Uuid,
    pub workflow_id: Uuid,
    pub state_json: String,
    pub status: InstanceStatus,
}

/// Instance execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "instance_status", rename_all = "lowercase")]
pub enum InstanceStatus {
    Running,
    Completed,
    Failed,
}

/// Queued action for worker dispatch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedAction {
    pub id: Uuid,
    pub instance_id: Uuid,
    pub node_id: String,
    pub dispatch_json: String,
    pub timeout_seconds: i32,
    pub max_retries: i32,
    pub attempt: i32,
}

/// Action completion record
#[derive(Debug, Clone)]
pub struct ActionCompletion {
    pub action_id: Uuid,
    pub node_id: String,
    pub instance_id: Uuid,
    pub success: bool,
    pub result: Option<Value>,
}

/// The workflow store - handles all persistence operations
pub struct Store {
    pool: Pool<Postgres>,
}

impl Store {
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Initialize database schema
    pub async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            -- Workflow definitions
            CREATE TABLE IF NOT EXISTS workflows (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                dag_json JSONB NOT NULL,
                concurrent BOOLEAN NOT NULL DEFAULT false,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Workflow instances
            CREATE TABLE IF NOT EXISTS instances (
                id UUID PRIMARY KEY,
                workflow_id UUID NOT NULL REFERENCES workflows(id),
                state_json JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                workflow_input BYTEA,
                result BYTEA,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );

            -- Action queue
            CREATE TABLE IF NOT EXISTS action_queue (
                id UUID PRIMARY KEY,
                instance_id UUID NOT NULL REFERENCES instances(id),
                node_id TEXT NOT NULL,
                dispatch_json JSONB NOT NULL,
                timeout_seconds INTEGER NOT NULL DEFAULT 300,
                max_retries INTEGER NOT NULL DEFAULT 3,
                attempt INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );

            -- Index for dequeuing
            CREATE INDEX IF NOT EXISTS idx_action_queue_pending
                ON action_queue(status, created_at)
                WHERE status = 'pending';

            -- Index for instance lookup
            CREATE INDEX IF NOT EXISTS idx_action_queue_instance
                ON action_queue(instance_id);
            "#,
        )
        .execute(&self.pool)
        .await
        .context("Failed to initialize schema")?;

        Ok(())
    }

    /// Register a new workflow and create an instance
    pub async fn register_workflow(
        &self,
        registration: &WorkflowRegistration,
    ) -> Result<(Uuid, Uuid)> {
        let ir = registration.ir.as_ref()
            .ok_or_else(|| anyhow!("Registration missing IR"))?;

        // Convert IR to DAG
        let dag = convert_workflow(ir, registration.concurrent)
            .map_err(|e| anyhow!("Failed to convert IR to DAG: {}", e))?;

        let dag_json = serde_json::to_string(&dag)
            .context("Failed to serialize DAG")?;

        // Create workflow record
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO workflows (id, name, dag_json, concurrent) VALUES ($1, $2, $3, $4)",
        )
        .bind(workflow_id)
        .bind(&registration.workflow_name)
        .bind(&dag_json)
        .bind(registration.concurrent)
        .execute(&self.pool)
        .await
        .context("Failed to insert workflow")?;

        // Create initial instance state
        let initial_context = decode_workflow_args(registration.initial_context.as_ref());
        let state = InstanceState::new(&dag, initial_context);
        let state_json = serde_json::to_string(&state)
            .context("Failed to serialize state")?;

        // Encode workflow input for storage
        let workflow_input_bytes = registration.initial_context.as_ref()
            .map(|args| prost::Message::encode_to_vec(args));

        // Create instance record
        let instance_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO instances (id, workflow_id, state_json, status, workflow_input) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(instance_id)
        .bind(workflow_id)
        .bind(&state_json)
        .bind("running")
        .bind(workflow_input_bytes)
        .execute(&self.pool)
        .await
        .context("Failed to insert instance")?;

        // Schedule initial ready nodes
        self.schedule_ready_nodes(instance_id, &dag, &state).await?;

        Ok((workflow_id, instance_id))
    }

    /// Schedule ready nodes from an instance state
    async fn schedule_ready_nodes(
        &self,
        instance_id: Uuid,
        dag: &Dag,
        state: &InstanceState,
    ) -> Result<()> {
        let scheduler = Scheduler::new(dag);

        // Get workflow input from database
        let row: Option<(Option<Vec<u8>>,)> = sqlx::query_as(
            "SELECT workflow_input FROM instances WHERE id = $1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await?;

        let workflow_input = row
            .and_then(|(bytes,)| bytes)
            .map(|bytes| prost::Message::decode(bytes.as_slice()).ok())
            .flatten()
            .unwrap_or_default();

        for node_id in state.ready_nodes() {
            let node = match dag.get_node(&node_id) {
                Some(n) => n,
                None => continue,
            };

            // Process all ready nodes through the iterative handler
            // This handles both scheduler-evaluated nodes and dispatch nodes
            let mut state_copy = state.clone();
            let action = scheduler.process_ready_node(
                &mut state_copy,
                &node_id,
                &workflow_input,
            )?;

            match action {
                SchedulerAction::Dispatch { node_id: dispatch_node_id, dispatch } => {
                    self.enqueue_action(instance_id, &dispatch_node_id, &dispatch, node).await?;
                }
                SchedulerAction::NodesReady { node_ids } => {
                    // Recursively process newly ready nodes
                    self.process_ready_nodes(instance_id, node_ids).await?;
                }
                SchedulerAction::WorkflowComplete { result } => {
                    self.complete_instance(instance_id, result).await?;
                }
                SchedulerAction::Idle => {}
            }
        }

        Ok(())
    }

    /// Enqueue an action for worker dispatch
    async fn enqueue_action(
        &self,
        instance_id: Uuid,
        node_id: &str,
        dispatch: &NodeDispatch,
        node: &crate::dag::Node,
    ) -> Result<Uuid> {
        let action_id = Uuid::new_v4();
        let dispatch_json = serde_json::to_string(dispatch)
            .context("Failed to serialize dispatch")?;

        let timeout = node.timeout_seconds.unwrap_or(300) as i32;
        let max_retries = node.max_retries.unwrap_or(3) as i32;

        sqlx::query(
            r#"
            INSERT INTO action_queue (id, instance_id, node_id, dispatch_json, timeout_seconds, max_retries)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(action_id)
        .bind(instance_id)
        .bind(node_id)
        .bind(&dispatch_json)
        .bind(timeout)
        .bind(max_retries)
        .execute(&self.pool)
        .await
        .context("Failed to enqueue action")?;

        Ok(action_id)
    }

    /// Dequeue an action for a worker
    pub async fn dequeue_action(&self) -> Result<Option<QueuedAction>> {
        let row = sqlx::query(
            r#"
            UPDATE action_queue
            SET status = 'running', started_at = NOW(), attempt = attempt + 1
            WHERE id = (
                SELECT id FROM action_queue
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, instance_id, node_id, dispatch_json, timeout_seconds, max_retries, attempt
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .context("Failed to dequeue action")?;

        match row {
            Some(row) => Ok(Some(QueuedAction {
                id: row.get("id"),
                instance_id: row.get("instance_id"),
                node_id: row.get("node_id"),
                dispatch_json: row.get("dispatch_json"),
                timeout_seconds: row.get("timeout_seconds"),
                max_retries: row.get("max_retries"),
                attempt: row.get("attempt"),
            })),
            None => Ok(None),
        }
    }

    /// Complete an action and process the result
    pub async fn complete_action(&self, completion: ActionCompletion) -> Result<()> {
        // Mark action as completed
        sqlx::query(
            r#"
            UPDATE action_queue
            SET status = $2, completed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(completion.action_id)
        .bind(if completion.success { "completed" } else { "failed" })
        .execute(&self.pool)
        .await
        .context("Failed to update action")?;

        // Load instance state and DAG
        let (state, dag, _workflow_input) = self.load_instance(completion.instance_id).await?;
        let mut state = state;

        // Process completion through scheduler
        let scheduler = Scheduler::new(&dag);
        let action = scheduler.handle_completion(
            &mut state,
            &completion.node_id,
            completion.result,
            completion.success,
        )?;

        // Save updated state
        self.save_instance_state(completion.instance_id, &state).await?;

        // Handle scheduler action
        match action {
            SchedulerAction::NodesReady { node_ids } => {
                self.process_ready_nodes(completion.instance_id, node_ids).await?;
            }
            SchedulerAction::WorkflowComplete { result } => {
                self.complete_instance(completion.instance_id, result).await?;
            }
            SchedulerAction::Dispatch { node_id, dispatch } => {
                if let Some(node) = dag.get_node(&node_id) {
                    self.enqueue_action(completion.instance_id, &node_id, &dispatch, node).await?;
                }
            }
            SchedulerAction::Idle => {}
        }

        Ok(())
    }

    /// Process all ready nodes iteratively (avoids recursive async)
    async fn process_ready_nodes(&self, instance_id: Uuid, initial_node_ids: Vec<String>) -> Result<()> {
        let mut pending = initial_node_ids;

        while let Some(node_id) = pending.pop() {
            let (mut state, dag, workflow_input) = self.load_instance(instance_id).await?;

            if dag.get_node(&node_id).is_none() {
                continue;
            }

            let scheduler = Scheduler::new(&dag);
            let action = scheduler.process_ready_node(&mut state, &node_id, &workflow_input)?;

            // Save updated state
            self.save_instance_state(instance_id, &state).await?;

            // Handle resulting action
            match action {
                SchedulerAction::NodesReady { node_ids } => {
                    // Add newly ready nodes to pending queue
                    pending.extend(node_ids);
                }
                SchedulerAction::WorkflowComplete { result } => {
                    self.complete_instance(instance_id, result).await?;
                    return Ok(()); // Workflow done
                }
                SchedulerAction::Dispatch { node_id: dispatch_node_id, dispatch } => {
                    if let Some(dispatch_node) = dag.get_node(&dispatch_node_id) {
                        self.enqueue_action(instance_id, &dispatch_node_id, &dispatch, dispatch_node).await?;
                    }
                }
                SchedulerAction::Idle => {}
            }
        }

        Ok(())
    }

    /// Load instance state and DAG
    async fn load_instance(&self, instance_id: Uuid) -> Result<(InstanceState, Dag, WorkflowArguments)> {
        let row = sqlx::query(
            r#"
            SELECT i.state_json, w.dag_json, i.workflow_input
            FROM instances i
            JOIN workflows w ON i.workflow_id = w.id
            WHERE i.id = $1
            "#,
        )
        .bind(instance_id)
        .fetch_one(&self.pool)
        .await
        .context("Failed to load instance")?;

        let state_json: String = row.get("state_json");
        let dag_json: String = row.get("dag_json");
        let workflow_input_bytes: Option<Vec<u8>> = row.get("workflow_input");

        let state: InstanceState = serde_json::from_str(&state_json)
            .context("Failed to deserialize state")?;
        let dag: Dag = serde_json::from_str(&dag_json)
            .context("Failed to deserialize DAG")?;
        let workflow_input: WorkflowArguments = workflow_input_bytes
            .map(|bytes| prost::Message::decode(bytes.as_slice()).ok())
            .flatten()
            .unwrap_or_default();

        Ok((state, dag, workflow_input))
    }

    /// Save instance state
    async fn save_instance_state(&self, instance_id: Uuid, state: &InstanceState) -> Result<()> {
        let state_json = serde_json::to_string(state)
            .context("Failed to serialize state")?;

        sqlx::query("UPDATE instances SET state_json = $2 WHERE id = $1")
            .bind(instance_id)
            .bind(&state_json)
            .execute(&self.pool)
            .await
            .context("Failed to save state")?;

        Ok(())
    }

    /// Mark instance as completed
    async fn complete_instance(&self, instance_id: Uuid, result: Option<Value>) -> Result<()> {
        let result_bytes = result.map(|v| serde_json::to_vec(&v).ok()).flatten();

        sqlx::query(
            r#"
            UPDATE instances
            SET status = 'completed', completed_at = NOW(), result = $2
            WHERE id = $1
            "#,
        )
        .bind(instance_id)
        .bind(result_bytes)
        .execute(&self.pool)
        .await
        .context("Failed to complete instance")?;

        Ok(())
    }

    /// Get instance status and result
    pub async fn get_instance(&self, instance_id: Uuid) -> Result<Option<(String, Option<Value>)>> {
        let row = sqlx::query(
            "SELECT status, result FROM instances WHERE id = $1",
        )
        .bind(instance_id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to get instance")?;

        match row {
            Some(row) => {
                let status: String = row.get("status");
                let result_bytes: Option<Vec<u8>> = row.get("result");
                let result = result_bytes
                    .map(|bytes| serde_json::from_slice(&bytes).ok())
                    .flatten();
                Ok(Some((status, result)))
            }
            None => Ok(None),
        }
    }

    /// Wait for instance completion
    pub async fn wait_for_instance(&self, instance_id: Uuid) -> Result<Option<Value>> {
        loop {
            match self.get_instance(instance_id).await? {
                Some((status, result)) if status == "completed" => {
                    return Ok(result);
                }
                Some((status, _)) if status == "failed" => {
                    return Err(anyhow!("Instance failed"));
                }
                None => {
                    return Err(anyhow!("Instance not found"));
                }
                _ => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

/// Decode WorkflowArguments to HashMap
fn decode_workflow_args(args: Option<&WorkflowArguments>) -> HashMap<String, Value> {
    let mut result = HashMap::new();

    if let Some(args) = args {
        for arg in &args.arguments {
            if let Some(value) = &arg.value {
                result.insert(arg.key.clone(), decode_arg_value(value));
            }
        }
    }

    result
}

/// Decode a single WorkflowArgumentValue to JSON Value
fn decode_arg_value(value: &crate::messages::proto::WorkflowArgumentValue) -> Value {
    use crate::messages::proto::workflow_argument_value::Kind;
    use crate::messages::proto::primitive_workflow_argument::Kind as PrimitiveKind;

    match &value.kind {
        Some(Kind::Primitive(p)) => match &p.kind {
            Some(PrimitiveKind::StringValue(s)) => Value::String(s.clone()),
            Some(PrimitiveKind::IntValue(i)) => Value::Number((*i).into()),
            Some(PrimitiveKind::DoubleValue(d)) => {
                serde_json::Number::from_f64(*d)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
            Some(PrimitiveKind::BoolValue(b)) => Value::Bool(*b),
            Some(PrimitiveKind::NullValue(_)) => Value::Null,
            None => Value::Null,
        },
        Some(Kind::ListValue(list)) => {
            Value::Array(list.items.iter().map(decode_arg_value).collect())
        }
        Some(Kind::TupleValue(tuple)) => {
            Value::Array(tuple.items.iter().map(decode_arg_value).collect())
        }
        Some(Kind::DictValue(dict)) => {
            let map: serde_json::Map<String, Value> = dict.entries.iter()
                .filter_map(|entry| {
                    entry.value.as_ref().map(|v| (entry.key.clone(), decode_arg_value(v)))
                })
                .collect();
            Value::Object(map)
        }
        Some(Kind::Basemodel(bm)) => {
            // Convert BaseModel to object with __type__ marker
            let mut map = serde_json::Map::new();
            map.insert("__type__".to_string(), Value::String(format!("{}.{}", bm.module, bm.name)));
            if let Some(data) = &bm.data {
                for entry in &data.entries {
                    if let Some(v) = &entry.value {
                        map.insert(entry.key.clone(), decode_arg_value(v));
                    }
                }
            }
            Value::Object(map)
        }
        Some(Kind::Exception(e)) => {
            let mut map = serde_json::Map::new();
            map.insert("__error__".to_string(), Value::Bool(true));
            map.insert("type".to_string(), Value::String(e.r#type.clone()));
            map.insert("module".to_string(), Value::String(e.module.clone()));
            map.insert("message".to_string(), Value::String(e.message.clone()));
            Value::Object(map)
        }
        None => Value::Null,
    }
}

#[cfg(test)]
mod tests {

    // Tests would require a database connection
    // TODO: Add integration tests with test database
}
