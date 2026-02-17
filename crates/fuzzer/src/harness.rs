//! Execution harness for generated fuzz cases.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::generator::GeneratedCase;
use waymark::backends::{
    MemoryBackend, QueuedInstance, WorkflowRegistration, WorkflowRegistryBackend,
};
use waymark::messages::ast as ir;
use waymark::waymark_core::dag::convert_to_dag;
use waymark::waymark_core::ir_parser::parse_program;
use waymark::waymark_core::runloop::{RunLoop, RunLoopSupervisorConfig};
use waymark::waymark_core::runner::RunnerState;
use waymark::workers::{ActionCallable, InlineWorkerPool, WorkerPoolError};

pub async fn run_case(case_index: usize, case: &GeneratedCase) -> Result<()> {
    let program = parse_program(case.source.trim()).map_err(|err| {
        anyhow::anyhow!(
            "case {case_index} failed to parse: {err}\n--- program ---\n{}",
            case.source
        )
    })?;

    let dag = Arc::new(convert_to_dag(&program).map_err(|err| {
        anyhow::anyhow!(
            "case {case_index} failed to convert DAG: {err}\n--- program ---\n{}",
            case.source
        )
    })?);

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    let workflow_version_id = register_workflow(case_index, &backend, &program).await?;
    let instance_id = Uuid::new_v4();
    let queued = build_instance(instance_id, workflow_version_id, dag, case.base_input)?;
    queue
        .lock()
        .expect("fuzz queue lock poisoned")
        .push_back(queued);

    let worker_pool = InlineWorkerPool::new(action_registry());
    let mut runloop = RunLoop::new(
        worker_pool,
        backend.clone(),
        RunLoopSupervisorConfig {
            max_concurrent_instances: 8,
            executor_shards: 1,
            instance_done_batch_size: None,
            poll_interval: Duration::from_millis(5),
            persistence_interval: Duration::from_millis(20),
            lock_uuid: Uuid::new_v4(),
            lock_ttl: Duration::from_secs(15),
            lock_heartbeat: Duration::from_secs(5),
            evict_sleep_threshold: Duration::from_secs(10),
            skip_sleep: false,
            active_instance_gauge: None,
        },
    );

    tokio::time::timeout(Duration::from_secs(5), runloop.run())
        .await
        .context("runloop timed out")?
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    let done = backend
        .instances_done()
        .into_iter()
        .find(|entry| entry.executor_id == instance_id)
        .ok_or_else(|| anyhow::anyhow!("case {case_index} produced no completed instance"))?;

    if let Some(error) = done.error {
        bail!(
            "case {case_index} completed with error: {error}\n--- program ---\n{}",
            case.source
        );
    }
    if done.result.is_none() {
        bail!(
            "case {case_index} completed without result payload\n--- program ---\n{}",
            case.source
        );
    }

    if (case_index + 1).is_multiple_of(10) {
        println!("Completed fuzz cases: {}", case_index + 1);
    }

    Ok(())
}

async fn register_workflow(
    case_index: usize,
    backend: &MemoryBackend,
    program: &ir::Program,
) -> Result<Uuid> {
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: format!("fuzz_case_{case_index}"),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .map_err(|err| anyhow::anyhow!(err.to_string()))
}

fn build_instance(
    instance_id: Uuid,
    workflow_version_id: Uuid,
    dag: Arc<waymark::waymark_core::dag::DAG>,
    base: i64,
) -> Result<QueuedInstance> {
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    state
        .record_assignment(
            vec!["base".to_string()],
            &literal_int(base),
            None,
            Some(format!("input base = {base}")),
        )
        .map_err(|err| anyhow::anyhow!(err.0))?;

    let entry_template = dag
        .entry_node
        .clone()
        .ok_or_else(|| anyhow::anyhow!("DAG entry node not found"))?;
    let entry_exec = state
        .queue_template_node(&entry_template, None)
        .map_err(|err| anyhow::anyhow!(err.0))?;

    Ok(QueuedInstance {
        workflow_version_id,
        schedule_id: None,
        dag: None,
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id,
        scheduled_at: None,
    })
}

fn literal_int(value: i64) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Literal(ir::Literal {
            value: Some(ir::literal::Value::IntValue(value)),
        })),
        span: None,
    }
}

fn action_registry() -> HashMap<String, ActionCallable> {
    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "inc".to_string(),
        Arc::new(|kwargs| Box::pin(action_inc(kwargs))),
    );
    actions.insert(
        "double".to_string(),
        Arc::new(|kwargs| Box::pin(action_double(kwargs))),
    );
    actions.insert(
        "sum".to_string(),
        Arc::new(|kwargs| Box::pin(action_sum(kwargs))),
    );
    actions
}

async fn action_inc(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let value = get_i64(&kwargs, "value")?;
    Ok(Value::Number((value + 1).into()))
}

async fn action_double(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let value = get_i64(&kwargs, "value")?;
    Ok(Value::Number((value * 2).into()))
}

async fn action_sum(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let values = kwargs
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects array input"))?;
    let mut total = 0i64;
    for item in values {
        let value = item
            .as_i64()
            .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects integer elements"))?;
        total += value;
    }
    Ok(Value::Number(total.into()))
}

fn get_i64(kwargs: &HashMap<String, Value>, key: &str) -> Result<i64, WorkerPoolError> {
    kwargs
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| WorkerPoolError::new("ActionError", format!("missing integer '{key}'")))
}
