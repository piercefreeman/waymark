//! Benchmark finalize-style database writes without running workers.
//!
//! This simulates a large number of running instances and measures how long
//! the finalize DB writes take under load.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use clap::Parser;
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng, seq::SliceRandom};
use serde::Serialize;
use tracing::info;
use uuid::Uuid;

use rappel::messages::execution::{ExecutionGraph, ExecutionNode, NodeStatus};
use rappel::{
    DAG, DAGNode, Database, ExecutionEvent, ExecutionState, ExecutionStateMachine,
    WorkflowInstanceId, WorkflowVersionId,
};

type ExecutionGraphUpdate = (WorkflowInstanceId, Vec<u8>, Option<DateTime<Utc>>);
type InstanceFinalization = (WorkflowInstanceId, Option<Vec<u8>>, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum BenchmarkMode {
    Current,
    Checkpoint,
    PayloadStrip,
    EventLog,
}

impl BenchmarkMode {
    fn as_str(&self) -> &'static str {
        match self {
            BenchmarkMode::Current => "current",
            BenchmarkMode::Checkpoint => "checkpoint",
            BenchmarkMode::PayloadStrip => "payload-strip",
            BenchmarkMode::EventLog => "event-log",
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "finalize-bench", about = "Benchmark finalize DB writes")]
struct Args {
    /// Benchmark mode
    #[arg(long, value_enum, default_value = "current")]
    mode: BenchmarkMode,

    /// Database URL (defaults to RAPPEL_DATABASE_URL or docker-compose config)
    #[arg(long)]
    database_url: Option<String>,

    /// Number of instances to simulate
    #[arg(long, default_value = "2500")]
    instances: usize,

    /// Number of nodes per execution graph
    #[arg(long, default_value = "50")]
    nodes: usize,

    /// Bytes of payload per node (inputs/results)
    #[arg(long, default_value = "1024")]
    payload_bytes: usize,

    /// Result payload size in bytes for completed instances
    #[arg(long, default_value = "256")]
    result_payload_bytes: usize,

    /// Percentage of instances to complete (0.0-1.0)
    #[arg(long, default_value = "0.10")]
    complete_pct: f64,

    /// Percentage of instances to fail (0.0-1.0)
    #[arg(long, default_value = "0.05")]
    fail_pct: f64,

    /// Percentage of instances to release (sleep) (0.0-1.0)
    #[arg(long, default_value = "0.15")]
    release_pct: f64,

    /// Batch size for DB updates (matches completion_batch_size)
    #[arg(long, default_value = "200")]
    batch_size: usize,

    /// Lease duration seconds for simulated ownership
    #[arg(long, default_value = "60")]
    lease_seconds: i64,

    /// RNG seed for reproducibility
    #[arg(long, default_value = "7")]
    seed: u64,

    /// Cleanup generated rows after run
    #[arg(long, default_value = "true")]
    cleanup: bool,

    /// Fill payload bytes with random data (reduces compression)
    #[arg(long, default_value = "false")]
    payload_random: bool,

    /// Number of update iterations to run (simulates repeated finalize loops)
    #[arg(long, default_value = "1")]
    iterations: usize,

    /// Only persist updates every N iterations (checkpoint mode)
    #[arg(long, default_value = "1")]
    update_every: usize,

    /// Snapshot every N iterations (event-log mode)
    #[arg(long, default_value = "10")]
    snapshot_every: usize,

    /// Write JSON summary to the given file
    #[arg(long)]
    json_out: Option<std::path::PathBuf>,

    /// Optional label to include in JSON output
    #[arg(long)]
    label: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let database_url = args
        .database_url
        .clone()
        .or_else(|| std::env::var("RAPPEL_DATABASE_URL").ok())
        .unwrap_or_else(|| {
            "postgres://mountaineer:mountaineer@localhost:5433/mountaineer_daemons".to_string()
        });

    if args.complete_pct < 0.0
        || args.fail_pct < 0.0
        || args.release_pct < 0.0
        || args.complete_pct + args.fail_pct + args.release_pct > 1.0
    {
        return Err(anyhow!("percentages must be >= 0 and sum to <= 1.0"));
    }
    if args.mode == BenchmarkMode::EventLog && args.nodes == 0 {
        return Err(anyhow!("event-log mode requires nodes > 0"));
    }

    let db = Database::connect_with_pool_size(&database_url, 10)
        .await
        .context("connect database")?;

    let bench_name = format!("finalize_bench_{}", Uuid::new_v4());
    let owner_id = runner_id(&bench_name);

    let version_id = create_version(&db, &bench_name).await?;
    let instance_ids = create_instances(&db, &bench_name, version_id, args.instances).await?;

    let include_payload = matches!(
        args.mode,
        BenchmarkMode::Current | BenchmarkMode::Checkpoint | BenchmarkMode::EventLog
    );
    let base_state = build_execution_state(
        args.nodes,
        args.payload_bytes,
        args.payload_random,
        args.seed,
        include_payload,
    );
    let graph_bytes = base_state.to_bytes();
    let dag = if args.mode == BenchmarkMode::EventLog {
        Some(build_bench_dag(args.nodes))
    } else {
        None
    };
    let mut state_machine = dag
        .as_ref()
        .map(|dag_ref| ExecutionStateMachine::new(base_state, Some(dag_ref)));

    assign_ownership(
        &db,
        &instance_ids,
        &graph_bytes,
        args.lease_seconds,
        &owner_id,
    )
    .await?;

    let distribution = split_instances(
        &instance_ids,
        args.complete_pct,
        args.fail_pct,
        args.release_pct,
        args.seed,
    );
    let node_ids = (0..args.nodes)
        .map(|i| format!("node_{i}"))
        .collect::<Vec<String>>();

    let result_payload = vec![b'r'; args.result_payload_bytes];
    let next_wakeup = Some(Utc::now() + ChronoDuration::minutes(10));

    let to_complete = distribution
        .complete
        .iter()
        .map(|id| (*id, Some(result_payload.clone()), graph_bytes.clone()))
        .collect::<Vec<InstanceFinalization>>();
    let to_fail = distribution
        .fail
        .iter()
        .map(|id| (*id, None, graph_bytes.clone()))
        .collect::<Vec<InstanceFinalization>>();
    let to_release = distribution
        .release
        .iter()
        .map(|id| (*id, graph_bytes.clone(), next_wakeup))
        .collect::<Vec<ExecutionGraphUpdate>>();
    let update_ids = distribution.update.clone();

    let bench_tables = create_bench_tables(&db, args.mode).await?;

    info!(
        mode = args.mode.as_str(),
        instances = args.instances,
        complete = to_complete.len(),
        fail = to_fail.len(),
        release = to_release.len(),
        update = update_ids.len(),
        batch_size = args.batch_size,
        iterations = args.iterations,
        "starting finalize benchmark"
    );

    let total_start = Instant::now();
    let mut complete_time = Duration::ZERO;
    let mut fail_time = Duration::ZERO;
    let mut release_time = Duration::ZERO;
    let mut update_time = Duration::ZERO;
    let mut action_logs_time = Duration::ZERO;
    let mut events_time = Duration::ZERO;
    let mut snapshot_time = Duration::ZERO;
    let mut delete_time = Duration::ZERO;
    let mut pending_delete = EventDeleteBatch {
        ids: Vec::new(),
        instance_ids: Vec::new(),
    };

    let mut rng = StdRng::seed_from_u64(args.seed ^ 0xD1B54A32D192ED03);

    if args.mode == BenchmarkMode::PayloadStrip
        && let Some(table) = bench_tables.action_logs.as_ref()
    {
        let start = Instant::now();
        let config = ActionLogConfig {
            payload_bytes: args.payload_bytes,
            result_payload_bytes: args.result_payload_bytes,
            payload_random: args.payload_random,
        };
        insert_action_logs(&db, table, &distribution, &config, &mut rng).await?;
        action_logs_time += start.elapsed();
    }

    if args.mode == BenchmarkMode::EventLog
        && let Some(table) = bench_tables.events.as_ref()
    {
        let start = Instant::now();
        let event_config = EventInsertConfig {
            node_ids: &node_ids,
            result_payload_bytes: args.result_payload_bytes,
            payload_random: args.payload_random,
            next_wakeup,
        };
        let batch = insert_events(&db, table, &distribution, &event_config, &mut rng).await?;
        if let Some(machine) = state_machine.as_mut() {
            machine.apply_all(batch.events.iter())?;
        }
        pending_delete.ids.extend(batch.ids);
        pending_delete.instance_ids.extend(batch.instance_ids);
        events_time += start.elapsed();
    } else {
        if !to_complete.is_empty() {
            for chunk in to_complete.chunks(args.batch_size.max(1)) {
                let start = Instant::now();
                db.complete_instances_batch(&owner_id, chunk)
                    .await
                    .context("complete instances batch")?;
                complete_time += start.elapsed();
            }
        }

        if !to_fail.is_empty() {
            for chunk in to_fail.chunks(args.batch_size.max(1)) {
                let start = Instant::now();
                db.fail_instances_batch(&owner_id, chunk)
                    .await
                    .context("fail instances batch")?;
                fail_time += start.elapsed();
            }
        }

        if !to_release.is_empty() {
            for chunk in to_release.chunks(args.batch_size.max(1)) {
                let start = Instant::now();
                db.release_instances_batch(&owner_id, chunk)
                    .await
                    .context("release instances batch")?;
                release_time += start.elapsed();
            }
        }
    }

    let iterations = args.iterations.max(1);
    let update_every = args.update_every.max(1);
    let snapshot_every = args.snapshot_every.max(1);

    for iter in 0..iterations {
        match args.mode {
            BenchmarkMode::Current => {
                if !update_ids.is_empty() {
                    for chunk in update_ids.chunks(args.batch_size.max(1)) {
                        let updates = build_update_chunk(chunk, &graph_bytes, None);
                        let start = Instant::now();
                        db.update_execution_graphs_batch(&owner_id, &updates)
                            .await
                            .context("update execution graphs batch")?;
                        update_time += start.elapsed();
                    }
                }
            }
            BenchmarkMode::Checkpoint => {
                let should_update = (iter + 1) % update_every == 0 || iter + 1 == iterations;
                if should_update && !update_ids.is_empty() {
                    for chunk in update_ids.chunks(args.batch_size.max(1)) {
                        let updates = build_update_chunk(chunk, &graph_bytes, None);
                        let start = Instant::now();
                        db.update_execution_graphs_batch(&owner_id, &updates)
                            .await
                            .context("update execution graphs batch")?;
                        update_time += start.elapsed();
                    }
                }
            }
            BenchmarkMode::PayloadStrip => {
                if !update_ids.is_empty() {
                    for chunk in update_ids.chunks(args.batch_size.max(1)) {
                        let updates = build_update_chunk(chunk, &graph_bytes, None);
                        let start = Instant::now();
                        db.update_execution_graphs_batch(&owner_id, &updates)
                            .await
                            .context("update execution graphs batch")?;
                        update_time += start.elapsed();
                    }
                }
            }
            BenchmarkMode::EventLog => {
                if let Some(table) = bench_tables.events.as_ref() {
                    let start = Instant::now();
                    let batch = insert_progress_events(
                        &db,
                        table,
                        &distribution.update,
                        &node_ids,
                        args.payload_bytes,
                        args.payload_random,
                        &mut rng,
                    )
                    .await?;
                    if let Some(machine) = state_machine.as_mut() {
                        machine.apply_all(batch.events.iter())?;
                    }
                    pending_delete.ids.extend(batch.ids);
                    pending_delete.instance_ids.extend(batch.instance_ids);
                    events_time += start.elapsed();
                }

                let should_snapshot = (iter + 1) % snapshot_every == 0 || iter + 1 == iterations;
                if should_snapshot && !update_ids.is_empty() {
                    let snapshot_bytes = if let Some(machine) = state_machine.as_ref() {
                        machine.state().to_bytes()
                    } else {
                        graph_bytes.clone()
                    };
                    for chunk in update_ids.chunks(args.batch_size.max(1)) {
                        let updates = build_update_chunk(chunk, &snapshot_bytes, None);
                        let start = Instant::now();
                        db.update_execution_graphs_batch(&owner_id, &updates)
                            .await
                            .context("snapshot update batch")?;
                        snapshot_time += start.elapsed();
                    }

                    if let Some(table) = bench_tables.events.as_ref() {
                        let start = Instant::now();
                        delete_event_batch(&db, table, &pending_delete).await?;
                        delete_time += start.elapsed();
                        pending_delete.ids.clear();
                        pending_delete.instance_ids.clear();
                    }
                }
            }
        }
    }

    let total_time = total_start.elapsed();

    info!(
        complete_ms = complete_time.as_millis(),
        fail_ms = fail_time.as_millis(),
        release_ms = release_time.as_millis(),
        update_ms = update_time.as_millis(),
        action_logs_ms = action_logs_time.as_millis(),
        events_ms = events_time.as_millis(),
        snapshot_ms = snapshot_time.as_millis(),
        delete_ms = delete_time.as_millis(),
        total_ms = total_time.as_millis(),
        "finalize benchmark complete"
    );

    let result = BenchResult {
        graph_bytes: graph_bytes.len(),
        complete_count: to_complete.len(),
        fail_count: to_fail.len(),
        release_count: to_release.len(),
        update_count: update_ids.len(),
        complete_time,
        fail_time,
        release_time,
        update_time,
        action_logs_time,
        events_time,
        snapshot_time,
        delete_time,
        total_time,
    };

    let summary = BenchmarkSummary::new(&args, &bench_name, &database_url, &result);

    if let Some(path) = &args.json_out {
        let json = serde_json::to_string_pretty(&summary)?;
        std::fs::write(path, json)?;
        info!(path = %path.display(), "wrote JSON summary");
    } else {
        let json = serde_json::to_string_pretty(&summary)?;
        println!("{json}");
    }

    if args.cleanup {
        cleanup(&db, &bench_name, &bench_tables).await?;
    }

    Ok(())
}

fn build_execution_state(
    nodes: usize,
    payload_bytes: usize,
    payload_random: bool,
    seed: u64,
    include_payload: bool,
) -> ExecutionState {
    let static_payload = if payload_random {
        Vec::new()
    } else {
        vec![b'a'; payload_bytes]
    };
    let mut rng = StdRng::seed_from_u64(seed ^ 0x9E3779B97F4A7C15);
    let mut nodes_map = HashMap::with_capacity(nodes);

    for i in 0..nodes {
        let status = match i % 10 {
            0..=4 => NodeStatus::Completed,
            5 => NodeStatus::Running,
            6 => NodeStatus::Pending,
            7 => NodeStatus::Blocked,
            _ => NodeStatus::Failed,
        };

        let node_id = format!("node_{i}");
        let mut node = ExecutionNode {
            template_id: node_id.clone(),
            status: status as i32,
            ..Default::default()
        };

        match status {
            NodeStatus::Completed => {
                node.completed_at_ms = Some(1_000);
                node.duration_ms = Some(1_000);
                if include_payload {
                    let payload = if payload_random {
                        let mut bytes = vec![0u8; payload_bytes];
                        rng.fill_bytes(&mut bytes);
                        bytes
                    } else {
                        static_payload.clone()
                    };
                    node.result = Some(payload);
                }
            }
            NodeStatus::Running => {
                node.worker_id = Some("worker-1".to_string());
                node.started_at_ms = Some(1_000);
                if include_payload {
                    let payload = if payload_random {
                        let mut bytes = vec![0u8; payload_bytes];
                        rng.fill_bytes(&mut bytes);
                        bytes
                    } else {
                        static_payload.clone()
                    };
                    node.inputs = Some(payload);
                }
            }
            NodeStatus::Failed => {
                node.completed_at_ms = Some(1_000);
                node.error = Some("benchmark failure".to_string());
                node.error_type = Some("BenchmarkError".to_string());
            }
            _ => {}
        }

        nodes_map.insert(node_id, node);
    }

    let graph = ExecutionGraph {
        nodes: nodes_map,
        variables: HashMap::new(),
        ready_queue: Vec::new(),
        exceptions: HashMap::new(),
        next_wakeup_time: None,
    };

    ExecutionState { graph }
}

fn build_bench_dag(nodes: usize) -> DAG {
    let mut dag = DAG::new();
    for i in 0..nodes {
        let node_id = format!("node_{i}");
        let mut node = DAGNode::new(node_id.clone(), "action_call".to_string(), node_id.clone());
        node.action_name = Some("action".to_string());
        node.module_name = Some("module".to_string());
        node.is_input = i == 0;
        node.is_output = i + 1 == nodes;
        node.function_name = Some("main".to_string());
        dag.add_node(node);
    }
    dag
}

fn build_update_chunk(
    ids: &[WorkflowInstanceId],
    graph_bytes: &[u8],
    next_wakeup: Option<DateTime<Utc>>,
) -> Vec<ExecutionGraphUpdate> {
    ids.iter()
        .map(|id| (*id, graph_bytes.to_vec(), next_wakeup))
        .collect()
}

fn random_node_id(node_ids: &[String], rng: &mut StdRng) -> String {
    let idx = rng.gen_range(0..node_ids.len());
    node_ids[idx].clone()
}

fn build_dispatch_event(
    node_id: String,
    payload_bytes: usize,
    payload_random: bool,
    rng: &mut StdRng,
) -> ExecutionEvent {
    ExecutionEvent::Dispatch {
        node_id,
        worker_id: "worker-1".to_string(),
        inputs: Some(build_payload(payload_bytes, payload_random, rng)),
        started_at_ms: Some(Utc::now().timestamp_millis()),
    }
}

fn build_completion_event(
    node_id: String,
    success: bool,
    result_payload_bytes: usize,
    payload_random: bool,
    rng: &mut StdRng,
) -> ExecutionEvent {
    let (result, error, error_type) = if success {
        (
            Some(build_payload(result_payload_bytes, payload_random, rng)),
            None,
            None,
        )
    } else {
        (
            None,
            Some("benchmark error".to_string()),
            Some("BenchmarkError".to_string()),
        )
    };
    ExecutionEvent::Completion {
        node_id,
        success,
        result,
        error,
        error_type,
        worker_id: "worker-1".to_string(),
        duration_ms: 100,
        worker_duration_ms: Some(100),
    }
}

fn build_wakeup_event(next_wakeup: Option<DateTime<Utc>>) -> ExecutionEvent {
    ExecutionEvent::SetNextWakeup {
        next_wakeup_ms: next_wakeup.map(|dt| dt.timestamp_millis()),
    }
}

fn event_type(event: &ExecutionEvent) -> &'static str {
    match event {
        ExecutionEvent::Dispatch { .. } => "dispatch",
        ExecutionEvent::Completion { .. } => "completion",
        ExecutionEvent::SetNextWakeup { .. } => "set_next_wakeup",
    }
}

fn event_node_id(event: &ExecutionEvent) -> &str {
    match event {
        ExecutionEvent::Dispatch { node_id, .. } | ExecutionEvent::Completion { node_id, .. } => {
            node_id.as_str()
        }
        ExecutionEvent::SetNextWakeup { .. } => "wakeup",
    }
}

fn serialize_event(event: &ExecutionEvent) -> Result<Vec<u8>> {
    serde_json::to_vec(event).context("serialize execution event")
}

fn push_event_record(
    batch: &mut EventBatch,
    instance_id: Uuid,
    event: ExecutionEvent,
) -> Result<()> {
    let payload = serialize_event(&event)?;
    batch.ids.push(Uuid::new_v4());
    batch.instance_ids.push(instance_id);
    batch.node_ids.push(event_node_id(&event).to_string());
    batch.kinds.push(event_type(&event).to_string());
    batch.payloads.push(payload);
    batch.events.push(event);
    Ok(())
}

async fn insert_event_batch(db: &Database, table: &str, batch: &EventBatch) -> Result<()> {
    if batch.ids.is_empty() {
        return Ok(());
    }
    let query = format!(
        r#"
        INSERT INTO {table} (id, instance_id, node_id, event_type, payload)
        SELECT * FROM UNNEST($1::uuid[], $2::uuid[], $3::text[], $4::text[], $5::bytea[])
        "#
    );
    sqlx::query(&query)
        .bind(&batch.ids)
        .bind(&batch.instance_ids)
        .bind(&batch.node_ids)
        .bind(&batch.kinds)
        .bind(&batch.payloads)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn delete_event_batch(db: &Database, table: &str, batch: &EventDeleteBatch) -> Result<()> {
    if batch.ids.is_empty() {
        return Ok(());
    }
    let query = format!(
        r#"
        DELETE FROM {table} t
        USING UNNEST($1::uuid[], $2::uuid[]) AS u(id, instance_id)
        WHERE t.id = u.id AND t.instance_id = u.instance_id
        "#
    );
    sqlx::query(&query)
        .bind(&batch.ids)
        .bind(&batch.instance_ids)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn create_version(db: &Database, workflow_name: &str) -> Result<WorkflowVersionId> {
    let version_id = db
        .upsert_workflow_version(workflow_name, workflow_name, b"benchmark", false)
        .await
        .context("create workflow version")?;
    Ok(version_id)
}

async fn create_instances(
    db: &Database,
    workflow_name: &str,
    version_id: WorkflowVersionId,
    count: usize,
) -> Result<Vec<WorkflowInstanceId>> {
    let payloads: Vec<Option<Vec<u8>>> = vec![None; count];
    let ids = db
        .create_instances_batch_with_priority(workflow_name, version_id, &payloads, None, 0)
        .await
        .context("create workflow instances")?;
    Ok(ids)
}

async fn assign_ownership(
    db: &Database,
    ids: &[WorkflowInstanceId],
    graph_bytes: &[u8],
    lease_seconds: i64,
    owner_id: &str,
) -> Result<()> {
    let id_values: Vec<Uuid> = ids.iter().map(|id| id.0).collect();
    let graphs: Vec<Vec<u8>> = id_values.iter().map(|_| graph_bytes.to_vec()).collect();

    sqlx::query(
        r#"
        UPDATE workflow_instances i
        SET execution_graph = u.execution_graph,
            owner_id = $2,
            lease_expires_at = NOW() + ($3 || ' seconds')::interval
        FROM (
            SELECT unnest($1::uuid[]) as id,
                   unnest($4::bytea[]) as execution_graph
        ) u
        WHERE i.id = u.id
        "#,
    )
    .bind(&id_values)
    .bind(owner_id)
    .bind(lease_seconds.to_string())
    .bind(&graphs)
    .execute(db.pool())
    .await
    .context("assign ownership")?;

    Ok(())
}

struct Distribution {
    complete: Vec<WorkflowInstanceId>,
    fail: Vec<WorkflowInstanceId>,
    release: Vec<WorkflowInstanceId>,
    update: Vec<WorkflowInstanceId>,
}

fn split_instances(
    ids: &[WorkflowInstanceId],
    complete_pct: f64,
    fail_pct: f64,
    release_pct: f64,
    seed: u64,
) -> Distribution {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut shuffled = ids.to_vec();
    shuffled.shuffle(&mut rng);

    let total = shuffled.len();
    let complete_count = (total as f64 * complete_pct).round() as usize;
    let fail_count = (total as f64 * fail_pct).round() as usize;
    let release_count = (total as f64 * release_pct).round() as usize;
    let mut idx = 0;

    let complete = shuffled[idx..idx + complete_count].to_vec();
    idx += complete_count;
    let fail = shuffled[idx..idx + fail_count].to_vec();
    idx += fail_count;
    let release = shuffled[idx..idx + release_count].to_vec();
    idx += release_count;
    let update = shuffled[idx..].to_vec();

    Distribution {
        complete,
        fail,
        release,
        update,
    }
}

struct BenchTables {
    action_logs: Option<String>,
    events: Option<String>,
}

struct ActionLogConfig {
    payload_bytes: usize,
    result_payload_bytes: usize,
    payload_random: bool,
}

struct EventInsertConfig<'a> {
    node_ids: &'a [String],
    result_payload_bytes: usize,
    payload_random: bool,
    next_wakeup: Option<DateTime<Utc>>,
}

struct EventBatch {
    ids: Vec<Uuid>,
    instance_ids: Vec<Uuid>,
    node_ids: Vec<String>,
    kinds: Vec<String>,
    payloads: Vec<Vec<u8>>,
    events: Vec<ExecutionEvent>,
}

struct EventDeleteBatch {
    ids: Vec<Uuid>,
    instance_ids: Vec<Uuid>,
}

async fn create_bench_tables(db: &Database, mode: BenchmarkMode) -> Result<BenchTables> {
    let suffix = Uuid::new_v4().simple().to_string();
    let mut tables = BenchTables {
        action_logs: None,
        events: None,
    };

    if mode == BenchmarkMode::PayloadStrip {
        let table = format!("bench_action_logs_{suffix}");
        let query = format!(
            r#"
            CREATE TABLE {table} (
                id BIGSERIAL PRIMARY KEY,
                instance_id UUID NOT NULL,
                node_id TEXT NOT NULL,
                payload BYTEA,
                result BYTEA,
                error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#
        );
        sqlx::query(&query).execute(db.pool()).await?;
        tables.action_logs = Some(table);
    }

    if mode == BenchmarkMode::EventLog {
        let table = format!("bench_events_{suffix}");
        let query = format!(
            r#"
            CREATE TABLE {table} (
                id UUID PRIMARY KEY,
                instance_id UUID NOT NULL,
                node_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload BYTEA,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#
        );
        sqlx::query(&query).execute(db.pool()).await?;
        tables.events = Some(table);
    }

    Ok(tables)
}

async fn insert_action_logs(
    db: &Database,
    table: &str,
    distribution: &Distribution,
    config: &ActionLogConfig,
    rng: &mut StdRng,
) -> Result<()> {
    let total = distribution.complete.len() + distribution.fail.len();
    if total == 0 {
        return Ok(());
    }

    let mut ids = Vec::with_capacity(total);
    let mut node_ids = Vec::with_capacity(total);
    let mut payloads = Vec::with_capacity(total);
    let mut results = Vec::with_capacity(total);
    let mut errors = Vec::with_capacity(total);

    for id in &distribution.complete {
        ids.push(id.0);
        node_ids.push("node_complete".to_string());
        payloads.push(build_payload(
            config.payload_bytes,
            config.payload_random,
            rng,
        ));
        results.push(build_payload(
            config.result_payload_bytes,
            config.payload_random,
            rng,
        ));
        errors.push(None);
    }

    for id in &distribution.fail {
        ids.push(id.0);
        node_ids.push("node_fail".to_string());
        payloads.push(build_payload(
            config.payload_bytes,
            config.payload_random,
            rng,
        ));
        results.push(Vec::new());
        errors.push(Some("benchmark error".to_string()));
    }

    let query = format!(
        r#"
        INSERT INTO {table} (instance_id, node_id, payload, result, error)
        SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::bytea[], $4::bytea[], $5::text[])
        "#
    );
    sqlx::query(&query)
        .bind(&ids)
        .bind(&node_ids)
        .bind(&payloads)
        .bind(&results)
        .bind(&errors)
        .execute(db.pool())
        .await?;

    Ok(())
}

async fn insert_events(
    db: &Database,
    table: &str,
    distribution: &Distribution,
    config: &EventInsertConfig<'_>,
    rng: &mut StdRng,
) -> Result<EventBatch> {
    if config.node_ids.is_empty() {
        return Err(anyhow!("nodes must be > 0 to build event logs"));
    }

    let total = distribution.complete.len() + distribution.fail.len() + distribution.release.len();
    if total == 0 {
        return Ok(EventBatch {
            ids: Vec::new(),
            instance_ids: Vec::new(),
            node_ids: Vec::new(),
            kinds: Vec::new(),
            payloads: Vec::new(),
            events: Vec::new(),
        });
    }

    let mut batch = EventBatch {
        ids: Vec::with_capacity(total),
        instance_ids: Vec::with_capacity(total),
        node_ids: Vec::with_capacity(total),
        kinds: Vec::with_capacity(total),
        payloads: Vec::with_capacity(total),
        events: Vec::with_capacity(total),
    };

    for id in &distribution.complete {
        let event = build_completion_event(
            random_node_id(config.node_ids, rng),
            true,
            config.result_payload_bytes,
            config.payload_random,
            rng,
        );
        push_event_record(&mut batch, id.0, event)?;
    }

    for id in &distribution.fail {
        let event = build_completion_event(
            random_node_id(config.node_ids, rng),
            false,
            config.result_payload_bytes,
            config.payload_random,
            rng,
        );
        push_event_record(&mut batch, id.0, event)?;
    }

    for id in &distribution.release {
        let event = build_wakeup_event(config.next_wakeup);
        push_event_record(&mut batch, id.0, event)?;
    }

    insert_event_batch(db, table, &batch).await?;
    Ok(batch)
}

async fn insert_progress_events(
    db: &Database,
    table: &str,
    update: &[WorkflowInstanceId],
    node_ids: &[String],
    payload_bytes: usize,
    payload_random: bool,
    rng: &mut StdRng,
) -> Result<EventBatch> {
    if node_ids.is_empty() {
        return Err(anyhow!("nodes must be > 0 to build event logs"));
    }
    if update.is_empty() {
        return Ok(EventBatch {
            ids: Vec::new(),
            instance_ids: Vec::new(),
            node_ids: Vec::new(),
            kinds: Vec::new(),
            payloads: Vec::new(),
            events: Vec::new(),
        });
    }

    let mut batch = EventBatch {
        ids: Vec::with_capacity(update.len()),
        instance_ids: Vec::with_capacity(update.len()),
        node_ids: Vec::with_capacity(update.len()),
        kinds: Vec::with_capacity(update.len()),
        payloads: Vec::with_capacity(update.len()),
        events: Vec::with_capacity(update.len()),
    };

    for id in update {
        let event = build_dispatch_event(
            random_node_id(node_ids, rng),
            payload_bytes,
            payload_random,
            rng,
        );
        push_event_record(&mut batch, id.0, event)?;
    }

    insert_event_batch(db, table, &batch).await?;
    Ok(batch)
}

fn build_payload(payload_bytes: usize, payload_random: bool, rng: &mut StdRng) -> Vec<u8> {
    if payload_bytes == 0 {
        return Vec::new();
    }
    if payload_random {
        let mut bytes = vec![0u8; payload_bytes];
        rng.fill_bytes(&mut bytes);
        bytes
    } else {
        vec![b'a'; payload_bytes]
    }
}

async fn cleanup(db: &Database, workflow_name: &str, tables: &BenchTables) -> Result<()> {
    if let Some(table) = &tables.action_logs {
        let query = format!("DROP TABLE IF EXISTS {table}");
        sqlx::query(&query).execute(db.pool()).await?;
    }
    if let Some(table) = &tables.events {
        let query = format!("DROP TABLE IF EXISTS {table}");
        sqlx::query(&query).execute(db.pool()).await?;
    }

    sqlx::query("DELETE FROM workflow_instances WHERE workflow_name = $1")
        .bind(workflow_name)
        .execute(db.pool())
        .await
        .context("cleanup workflow instances")?;

    sqlx::query("DELETE FROM workflow_versions WHERE workflow_name = $1")
        .bind(workflow_name)
        .execute(db.pool())
        .await
        .context("cleanup workflow versions")?;

    Ok(())
}

fn runner_id(name: &str) -> String {
    format!("finalize-bench-{name}")
}

struct BenchResult {
    graph_bytes: usize,
    complete_count: usize,
    fail_count: usize,
    release_count: usize,
    update_count: usize,
    complete_time: Duration,
    fail_time: Duration,
    release_time: Duration,
    update_time: Duration,
    action_logs_time: Duration,
    events_time: Duration,
    snapshot_time: Duration,
    delete_time: Duration,
    total_time: Duration,
}

#[derive(Serialize)]
struct BenchmarkSummary {
    mode: String,
    label: Option<String>,
    workflow_name: String,
    database_url: String,
    instances: usize,
    nodes: usize,
    payload_bytes: usize,
    result_payload_bytes: usize,
    complete_pct: f64,
    fail_pct: f64,
    release_pct: f64,
    batch_size: usize,
    lease_seconds: i64,
    seed: u64,
    payload_random: bool,
    iterations: usize,
    update_every: usize,
    snapshot_every: usize,
    graph_bytes: usize,
    complete_count: usize,
    fail_count: usize,
    release_count: usize,
    update_count: usize,
    complete_ms: u128,
    fail_ms: u128,
    release_ms: u128,
    update_ms: u128,
    action_logs_ms: u128,
    events_ms: u128,
    snapshot_ms: u128,
    delete_ms: u128,
    total_ms: u128,
}

impl BenchmarkSummary {
    fn new(args: &Args, workflow_name: &str, database_url: &str, result: &BenchResult) -> Self {
        Self {
            mode: args.mode.as_str().to_string(),
            label: args.label.clone(),
            workflow_name: workflow_name.to_string(),
            database_url: database_url.to_string(),
            instances: args.instances,
            nodes: args.nodes,
            payload_bytes: args.payload_bytes,
            result_payload_bytes: args.result_payload_bytes,
            complete_pct: args.complete_pct,
            fail_pct: args.fail_pct,
            release_pct: args.release_pct,
            batch_size: args.batch_size,
            lease_seconds: args.lease_seconds,
            seed: args.seed,
            payload_random: args.payload_random,
            iterations: args.iterations,
            update_every: args.update_every,
            snapshot_every: args.snapshot_every,
            graph_bytes: result.graph_bytes,
            complete_count: result.complete_count,
            fail_count: result.fail_count,
            release_count: result.release_count,
            update_count: result.update_count,
            complete_ms: result.complete_time.as_millis(),
            fail_ms: result.fail_time.as_millis(),
            release_ms: result.release_time.as_millis(),
            update_ms: result.update_time.as_millis(),
            action_logs_ms: result.action_logs_time.as_millis(),
            events_ms: result.events_time.as_millis(),
            snapshot_ms: result.snapshot_time.as_millis(),
            delete_ms: result.delete_time.as_millis(),
            total_ms: result.total_time.as_millis(),
        }
    }
}
