//! Benchmark for the finalization pipeline.
//!
//! This benchmark reproduces the actual finalize_completed_instances behavior:
//! 1. Maintains a set of "active instances" with realistic execution graphs
//! 2. Serializes + compresses graphs (the CPU-bound part)
//! 3. Batch updates to PostgreSQL (the I/O-bound part)
//!
//! Run with: cargo run --release --bin finalization_bench
//!
//! Example measuring baseline:
//!   cargo run --release --bin finalization_bench -- --instances 2500 --nodes 50
//!
//! The output shows timing breakdown similar to the runner's profile logs.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use chrono::{Duration as ChronoDuration, Utc};
use clap::Parser;
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use uuid::Uuid;

use rappel::db::{ExecutionGraphUpdate, InstanceFinalization, NodePayload};
use rappel::messages::execution::{
    AttemptRecord, BackoffConfig, BackoffKind, ExecutionGraph, ExecutionNode, NodeStatus,
};
use rappel::{Database, ExecutionState, WorkflowInstanceId};

#[derive(Parser, Debug)]
#[command(
    name = "finalization-bench",
    about = "Benchmark the finalization pipeline"
)]
struct Args {
    /// Database URL (defaults to RAPPEL_DATABASE_URL or docker-compose config)
    #[arg(long)]
    database_url: Option<String>,

    /// Number of active instances to simulate
    #[arg(long, default_value = "2500")]
    instances: usize,

    /// Number of nodes per execution graph
    #[arg(long, default_value = "50")]
    nodes: usize,

    /// Bytes of payload per node (inputs/results)
    #[arg(long, default_value = "1024")]
    payload_bytes: usize,

    /// Percentage of instances to complete per iteration (0.0-1.0)
    #[arg(long, default_value = "0.02")]
    complete_pct: f64,

    /// Percentage of instances to release (sleep) per iteration (0.0-1.0)
    #[arg(long, default_value = "0.01")]
    release_pct: f64,

    /// Percentage of instances that need graph updates per iteration (0.0-1.0)
    #[arg(long, default_value = "0.80")]
    update_pct: f64,

    /// Batch size for DB updates
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Lease duration seconds for simulated ownership
    #[arg(long, default_value = "60")]
    lease_seconds: i64,

    /// RNG seed for reproducibility
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Number of finalize iterations to run
    #[arg(long, default_value = "20")]
    iterations: usize,

    /// Cleanup generated rows after run
    #[arg(long, default_value = "true")]
    cleanup: bool,

    /// Fill payload bytes with random data (reduces compression ratio)
    #[arg(long, default_value = "false")]
    random_payloads: bool,

    /// Database connection pool size
    #[arg(long, default_value = "10")]
    pool_size: u32,

    /// Use stripped serialization (removes inputs from nodes)
    #[arg(long, default_value = "false")]
    strip_payloads: bool,

    /// Offload payloads to separate node_payloads table (full optimization)
    /// This writes payloads to node_payloads and stripped graphs to workflow_instances
    #[arg(long, default_value = "false")]
    offload_payloads: bool,

    /// Strip ALL payloads including results (maximum reduction)
    /// WARNING: Breaks barrier/join aggregation unless results are stored separately
    #[arg(long, default_value = "false")]
    fully_strip: bool,
}

/// Simulated active instance (mirrors the runner's ActiveInstance)
struct SimulatedInstance {
    instance_id: WorkflowInstanceId,
    state: ExecutionState,
}

/// Timing breakdown for a single finalize iteration
#[derive(Debug, Default)]
struct FinalizeTiming {
    /// Time to iterate instances and serialize graphs
    serialize_ms: u128,
    /// Time to execute complete_instances_batch calls
    complete_ms: u128,
    /// Time to execute release_instances_batch calls
    release_ms: u128,
    /// Time to execute update_execution_graphs_batch calls
    update_ms: u128,
    /// Time to write payloads to node_payloads table
    payloads_ms: u128,
    /// Number of instances processed
    complete_count: usize,
    release_count: usize,
    update_count: usize,
    /// Number of payloads written
    payloads_count: usize,
    /// Total bytes serialized (execution graphs)
    total_bytes: usize,
    /// Total bytes written to payloads table
    payload_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Validate percentages
    if args.complete_pct + args.release_pct + args.update_pct > 1.0 {
        return Err(anyhow!(
            "complete_pct + release_pct + update_pct must be <= 1.0"
        ));
    }

    let database_url = args
        .database_url
        .clone()
        .or_else(|| std::env::var("RAPPEL_DATABASE_URL").ok())
        .unwrap_or_else(|| {
            "postgres://mountaineer:mountaineer@localhost:5433/mountaineer_daemons".to_string()
        });

    println!("Connecting to database...");
    let db = Database::connect_with_pool_size(&database_url, args.pool_size)
        .await
        .context("connect database")?;

    let mut rng = StdRng::seed_from_u64(args.seed);

    // Create a unique workflow for this benchmark run
    let bench_name = format!("finalization_bench_{}", Uuid::new_v4());
    let owner_id = format!("finalization-bench-{}", Uuid::new_v4());

    println!("Setting up benchmark workflow: {}", bench_name);

    let version_id = db
        .upsert_workflow_version(&bench_name, &bench_name, b"benchmark", false)
        .await
        .context("create workflow version")?;

    // Create instances in the database
    println!("Creating {} instances in database...", args.instances);
    let input_payloads: Vec<Option<Vec<u8>>> = vec![None; args.instances];
    let instance_ids = db
        .create_instances_batch_with_priority(&bench_name, version_id, &input_payloads, None, 0)
        .await
        .context("create instances")?;

    // Claim all instances (set owner_id and lease)
    let instance_uuids: Vec<Uuid> = instance_ids.iter().map(|id| id.0).collect();
    let lease_expires_at = Utc::now() + ChronoDuration::seconds(args.lease_seconds);
    sqlx::query(
        r#"
        UPDATE workflow_instances
        SET owner_id = $1,
            lease_expires_at = $2
        WHERE id = ANY($3)
        "#,
    )
    .bind(&owner_id)
    .bind(lease_expires_at)
    .bind(&instance_uuids)
    .execute(db.pool())
    .await
    .context("assign instance leases")?;

    // Build template payloads
    let node_inputs = build_payload(args.payload_bytes, args.random_payloads, &mut rng);
    let node_results = build_payload(args.payload_bytes, args.random_payloads, &mut rng);

    // Create simulated active instances with realistic execution graphs
    println!(
        "Building {} execution graphs ({} nodes each, {} bytes/payload)...",
        args.instances, args.nodes, args.payload_bytes
    );
    let build_start = Instant::now();
    let mut active_instances: HashMap<Uuid, SimulatedInstance> = HashMap::new();

    for id in &instance_ids {
        let state = build_execution_state(args.nodes, &node_inputs, &node_results, &mut rng);
        active_instances.insert(
            id.0,
            SimulatedInstance {
                instance_id: *id,
                state,
            },
        );
    }
    let build_time = build_start.elapsed();
    println!("  Build time: {:?}", build_time);

    // Seed initial execution graphs in DB
    println!("Seeding initial execution graphs to database...");
    let seed_start = Instant::now();
    let updates: Vec<ExecutionGraphUpdate> = active_instances
        .values()
        .map(|inst| (inst.instance_id, inst.state.to_bytes(), None))
        .collect();

    for chunk in updates.chunks(args.batch_size.max(1)) {
        db.update_execution_graphs_batch(&owner_id, chunk)
            .await
            .context("seed execution graphs")?;
    }
    let seed_time = seed_start.elapsed();
    println!("  Seed time: {:?}", seed_time);

    // Calculate counts for each operation type
    let complete_count = ((args.instances as f64) * args.complete_pct).round() as usize;
    let release_count = ((args.instances as f64) * args.release_pct).round() as usize;
    let update_count = ((args.instances as f64) * args.update_pct).round() as usize;

    println!("\n=== Starting finalization benchmark ===");
    println!("  Instances: {}", args.instances);
    println!("  Nodes per graph: {}", args.nodes);
    println!("  Payload bytes per node: {}", args.payload_bytes);
    println!("  Batch size: {}", args.batch_size);
    println!("  Iterations: {}", args.iterations);
    println!("  Strip payloads: {}", args.strip_payloads);
    println!("  Offload payloads: {}", args.offload_payloads);
    println!(
        "  Per iteration: {} complete, {} release, {} update",
        complete_count, release_count, update_count
    );
    println!();

    let mut timings: Vec<FinalizeTiming> = Vec::new();
    let total_start = Instant::now();

    for iter in 0..args.iterations {
        let timing = run_finalize_iteration(
            &db,
            &owner_id,
            &mut active_instances,
            complete_count,
            release_count,
            update_count,
            args.batch_size,
            args.strip_payloads || args.offload_payloads || args.fully_strip,
            args.fully_strip,
            args.offload_payloads,
            &mut rng,
        )
        .await?;

        // Extend lease for remaining instances
        let lease_expires_at = Utc::now() + ChronoDuration::seconds(args.lease_seconds);
        let remaining_ids: Vec<Uuid> = active_instances.keys().copied().collect();
        if !remaining_ids.is_empty() {
            sqlx::query(
                r#"
                UPDATE workflow_instances
                SET lease_expires_at = $1
                WHERE id = ANY($2) AND owner_id = $3
                "#,
            )
            .bind(lease_expires_at)
            .bind(&remaining_ids)
            .bind(&owner_id)
            .execute(db.pool())
            .await?;
        }

        // Replenish completed/released instances to maintain steady state
        let replenish_count = timing.complete_count + timing.release_count;
        if replenish_count > 0 && iter < args.iterations - 1 {
            // Create new instances
            let new_payloads: Vec<Option<Vec<u8>>> = vec![None; replenish_count];
            let new_ids = db
                .create_instances_batch_with_priority(
                    &bench_name,
                    version_id,
                    &new_payloads,
                    None,
                    0,
                )
                .await?;

            // Claim them
            let new_uuids: Vec<Uuid> = new_ids.iter().map(|id| id.0).collect();
            sqlx::query(
                r#"
                UPDATE workflow_instances
                SET owner_id = $1, lease_expires_at = $2
                WHERE id = ANY($3)
                "#,
            )
            .bind(&owner_id)
            .bind(lease_expires_at)
            .bind(&new_uuids)
            .execute(db.pool())
            .await?;

            // Add to active set
            for id in new_ids {
                let state =
                    build_execution_state(args.nodes, &node_inputs, &node_results, &mut rng);
                active_instances.insert(
                    id.0,
                    SimulatedInstance {
                        instance_id: id,
                        state,
                    },
                );
            }
        }

        let total_ms = timing.serialize_ms
            + timing.complete_ms
            + timing.release_ms
            + timing.update_ms
            + timing.payloads_ms;

        if args.offload_payloads {
            println!(
                "iter={:2} ser={:4}ms cmp={:4}ms rel={:4}ms upd={:4}ms pay={:4}ms tot={:5}ms graph={:7} payload={:8} active={}",
                iter,
                timing.serialize_ms,
                timing.complete_ms,
                timing.release_ms,
                timing.update_ms,
                timing.payloads_ms,
                total_ms,
                timing.total_bytes,
                timing.payload_bytes,
                active_instances.len(),
            );
        } else {
            println!(
                "iter={:2} serialize={:5}ms complete={:5}ms release={:5}ms update={:5}ms total={:5}ms bytes={:8} active={}",
                iter,
                timing.serialize_ms,
                timing.complete_ms,
                timing.release_ms,
                timing.update_ms,
                total_ms,
                timing.total_bytes,
                active_instances.len(),
            );
        }

        timings.push(timing);
    }

    let total_time = total_start.elapsed();

    // Print summary statistics
    println!("\n=== Summary ===");
    println!("Total time: {:?}", total_time);

    let avg_serialize: u128 =
        timings.iter().map(|t| t.serialize_ms).sum::<u128>() / timings.len() as u128;
    let avg_complete: u128 =
        timings.iter().map(|t| t.complete_ms).sum::<u128>() / timings.len() as u128;
    let avg_release: u128 =
        timings.iter().map(|t| t.release_ms).sum::<u128>() / timings.len() as u128;
    let avg_update: u128 =
        timings.iter().map(|t| t.update_ms).sum::<u128>() / timings.len() as u128;
    let avg_payloads: u128 =
        timings.iter().map(|t| t.payloads_ms).sum::<u128>() / timings.len() as u128;
    let avg_total = avg_serialize + avg_complete + avg_release + avg_update + avg_payloads;
    let avg_bytes: usize = timings.iter().map(|t| t.total_bytes).sum::<usize>() / timings.len();
    let avg_payload_bytes: usize =
        timings.iter().map(|t| t.payload_bytes).sum::<usize>() / timings.len();

    println!("Average per iteration:");
    println!("  serialize: {}ms", avg_serialize);
    println!("  complete:  {}ms", avg_complete);
    println!("  release:   {}ms", avg_release);
    println!("  update:    {}ms", avg_update);
    if args.offload_payloads {
        println!("  payloads:  {}ms", avg_payloads);
    }
    println!("  total:     {}ms", avg_total);
    println!("  graph_bytes: {}", avg_bytes);
    if args.offload_payloads {
        println!("  payload_bytes: {}", avg_payload_bytes);
    }

    // Cleanup
    if args.cleanup {
        println!("\nCleaning up...");
        cleanup(&db, &bench_name).await?;
    }

    Ok(())
}

/// Run a single finalize iteration, mimicking finalize_completed_instances
#[allow(clippy::too_many_arguments)]
async fn run_finalize_iteration(
    db: &Database,
    owner_id: &str,
    active_instances: &mut HashMap<Uuid, SimulatedInstance>,
    complete_count: usize,
    release_count: usize,
    update_count: usize,
    batch_size: usize,
    strip_payloads: bool,
    fully_strip: bool,
    offload_payloads: bool,
    rng: &mut StdRng,
) -> Result<FinalizeTiming> {
    let mut timing = FinalizeTiming::default();

    // Phase 1: Iterate instances and build batch payloads (simulates the serialization phase)
    let serialize_start = Instant::now();

    let mut to_complete: Vec<InstanceFinalization> = Vec::new();
    let mut to_release: Vec<ExecutionGraphUpdate> = Vec::new();
    let mut to_update: Vec<ExecutionGraphUpdate> = Vec::new();
    let mut completed_ids: Vec<Uuid> = Vec::new();
    let mut released_ids: Vec<Uuid> = Vec::new();

    // Payloads to offload (when offload_payloads is true)
    let mut payloads_to_write: Vec<NodePayload> = Vec::new();

    let instance_ids: Vec<Uuid> = active_instances.keys().copied().collect();
    let mut shuffled = instance_ids.clone();
    shuffled.sort(); // Deterministic order for reproducibility

    let mut offset = 0;

    // Helper to serialize based on strip_payloads/fully_strip flags
    let serialize = |state: &ExecutionState| -> Vec<u8> {
        if fully_strip {
            state.to_bytes_fully_stripped()
        } else if strip_payloads {
            state.to_bytes_stripped()
        } else {
            state.to_bytes()
        }
    };

    // Helper to collect payloads from an instance's execution state
    let collect_payloads =
        |inst: &SimulatedInstance, payloads: &mut Vec<NodePayload>, timing: &mut FinalizeTiming| {
            for (node_id, node) in &inst.state.graph.nodes {
                if node.inputs.is_some() || node.result.is_some() {
                    let inputs_size = node.inputs.as_ref().map(|b| b.len()).unwrap_or(0);
                    let result_size = node.result.as_ref().map(|b| b.len()).unwrap_or(0);
                    timing.payload_bytes += inputs_size + result_size;

                    payloads.push(NodePayload {
                        instance_id: inst.instance_id,
                        node_id: node_id.clone(),
                        inputs: node.inputs.clone(),
                        result: node.result.clone(),
                    });
                }
            }
        };

    // Select instances to complete
    for &id in shuffled.iter().skip(offset).take(complete_count) {
        if let Some(inst) = active_instances.get(&id) {
            // For completed instances, we write their payloads to the offload table.
            // In real code, this would have been done incrementally as nodes completed.
            // Here we simulate writing them at finalization time.
            if offload_payloads {
                collect_payloads(inst, &mut payloads_to_write, &mut timing);
            }

            // Simulate completing the workflow - serialize the final state
            let graph_bytes = serialize(&inst.state);
            timing.total_bytes += graph_bytes.len();

            // Result payload (what the workflow returns)
            let result_payload = Some(vec![0u8; 256]);

            to_complete.push((inst.instance_id, result_payload, graph_bytes));
            completed_ids.push(id);
        }
    }
    offset += complete_count;

    // Select instances to release (entering durable sleep)
    for &id in shuffled.iter().skip(offset).take(release_count) {
        if let Some(inst) = active_instances.get(&id) {
            // Released instances: payloads were already written when nodes completed.
            // No new payload writes needed here.
            let graph_bytes = serialize(&inst.state);
            timing.total_bytes += graph_bytes.len();

            // Set a wakeup time
            let next_wakeup = Some(Utc::now() + ChronoDuration::seconds(60));

            to_release.push((inst.instance_id, graph_bytes, next_wakeup));
            released_ids.push(id);
        }
    }
    offset += release_count;

    // Select instances to update (state changed but not complete)
    for &id in shuffled.iter().skip(offset).take(update_count) {
        if let Some(inst) = active_instances.get_mut(&id) {
            // Simulate some state change - advance a node
            if let Some(node) = inst.state.graph.nodes.values_mut().next() {
                node.attempt_number += 1;
            }

            // Updated instances: only write payloads for newly completed nodes.
            // Simulate ~2% of updates having a newly completed node.
            if offload_payloads && rng.gen_bool(0.02) {
                // Write just one node's payload (simulating one node completion)
                if let Some((node_id, node)) = inst.state.graph.nodes.iter().next()
                    && (node.inputs.is_some() || node.result.is_some())
                {
                    let inputs_size = node.inputs.as_ref().map(|b| b.len()).unwrap_or(0);
                    let result_size = node.result.as_ref().map(|b| b.len()).unwrap_or(0);
                    timing.payload_bytes += inputs_size + result_size;
                    payloads_to_write.push(NodePayload {
                        instance_id: inst.instance_id,
                        node_id: node_id.clone(),
                        inputs: node.inputs.clone(),
                        result: node.result.clone(),
                    });
                }
            }

            let graph_bytes = serialize(&inst.state);
            timing.total_bytes += graph_bytes.len();

            let next_wakeup = if rng.gen_bool(0.1) {
                Some(Utc::now() + ChronoDuration::seconds(30))
            } else {
                None
            };

            to_update.push((inst.instance_id, graph_bytes, next_wakeup));
        }
    }

    timing.serialize_ms = serialize_start.elapsed().as_millis();
    timing.complete_count = to_complete.len();
    timing.release_count = to_release.len();
    timing.update_count = to_update.len();
    timing.payloads_count = payloads_to_write.len();

    // Phase 2: Execute batch database operations

    // Write payloads to node_payloads table (if offloading)
    if offload_payloads && !payloads_to_write.is_empty() {
        let payloads_start = Instant::now();
        for chunk in payloads_to_write.chunks(batch_size.max(1)) {
            db.save_node_payloads_batch(chunk).await?;
        }
        timing.payloads_ms = payloads_start.elapsed().as_millis();
    }

    // Complete instances
    let complete_start = Instant::now();
    for chunk in to_complete.chunks(batch_size.max(1)) {
        db.complete_instances_batch(owner_id, chunk).await?;
    }
    timing.complete_ms = complete_start.elapsed().as_millis();

    // Release instances
    let release_start = Instant::now();
    for chunk in to_release.chunks(batch_size.max(1)) {
        db.release_instances_batch(owner_id, chunk).await?;
    }
    timing.release_ms = release_start.elapsed().as_millis();

    // Update instances
    let update_start = Instant::now();
    for chunk in to_update.chunks(batch_size.max(1)) {
        db.update_execution_graphs_batch(owner_id, chunk).await?;
    }
    timing.update_ms = update_start.elapsed().as_millis();

    // Phase 3: Remove finalized instances from active set
    for id in completed_ids {
        active_instances.remove(&id);
    }
    for id in released_ids {
        active_instances.remove(&id);
    }

    Ok(timing)
}

/// Build a payload of the specified size
fn build_payload(size: usize, random: bool, rng: &mut StdRng) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    if random {
        rng.fill_bytes(&mut bytes);
    }
    bytes
}

/// Build a realistic execution state with the specified number of nodes
fn build_execution_state(
    nodes: usize,
    inputs_payload: &[u8],
    result_payload: &[u8],
    rng: &mut StdRng,
) -> ExecutionState {
    let mut graph = ExecutionGraph {
        nodes: HashMap::new(),
        variables: HashMap::new(),
        ready_queue: Vec::new(),
        exceptions: HashMap::new(),
        next_wakeup_time: None,
    };

    // Add some workflow variables (common in real workflows)
    for i in 0..5 {
        let var_name = format!("var_{}", i);
        let var_value = vec![0u8; 128];
        graph.variables.insert(var_name, var_value);
    }

    for idx in 0..nodes {
        let node_id = format!("action_{}", idx);

        // Simulate realistic status distribution
        let status = if idx < nodes / 2 {
            NodeStatus::Completed
        } else if idx < nodes * 3 / 4 {
            NodeStatus::Running
        } else {
            NodeStatus::Pending
        };

        let mut node = ExecutionNode {
            template_id: node_id.clone(),
            spread_index: None,
            status: status as i32,
            worker_id: if status == NodeStatus::Running || status == NodeStatus::Completed {
                Some(format!("worker_{}", rng.gen_range(0..4)))
            } else {
                None
            },
            started_at_ms: if status == NodeStatus::Running || status == NodeStatus::Completed {
                Some(Utc::now().timestamp_millis() - rng.gen_range(100..5000))
            } else {
                None
            },
            completed_at_ms: if status == NodeStatus::Completed {
                Some(Utc::now().timestamp_millis())
            } else {
                None
            },
            duration_ms: if status == NodeStatus::Completed {
                Some(rng.gen_range(10..500))
            } else {
                None
            },
            inputs: Some(inputs_payload.to_vec()),
            result: if status == NodeStatus::Completed {
                Some(result_payload.to_vec())
            } else {
                None
            },
            error: None,
            error_type: None,
            waiting_for: Vec::new(),
            completed_count: 0,
            attempt_number: rng.gen_range(1..3),
            max_retries: 3,
            attempts: Vec::new(),
            timeout_seconds: 300,
            timeout_retry_limit: 3,
            backoff: Some(BackoffConfig {
                kind: BackoffKind::Exponential as i32,
                base_delay_ms: 1000,
                multiplier: 2.0,
            }),
            loop_index: None,
            loop_accumulators: None,
            targets: vec![format!("result_{}", idx)],
        };

        // Add attempt history for completed nodes (realistic overhead)
        if status == NodeStatus::Completed && node.attempt_number > 1 {
            for attempt in 1..node.attempt_number {
                node.attempts.push(AttemptRecord {
                    attempt_number: attempt,
                    worker_id: format!("worker_{}", rng.gen_range(0..4)),
                    started_at_ms: Utc::now().timestamp_millis() - 10000,
                    completed_at_ms: Utc::now().timestamp_millis() - 9000,
                    duration_ms: rng.gen_range(100..1000),
                    success: false,
                    result: None,
                    error: Some("Timeout".to_string()),
                    error_type: Some("TimeoutError".to_string()),
                });
            }
        }

        graph.nodes.insert(node_id, node);
    }

    // Add some nodes to ready queue
    for i in (nodes * 3 / 4)..nodes {
        graph.ready_queue.push(format!("action_{}", i));
    }

    ExecutionState { graph }
}

async fn cleanup(db: &Database, workflow_name: &str) -> Result<()> {
    sqlx::query("DELETE FROM workflow_instances WHERE workflow_name = $1")
        .bind(workflow_name)
        .execute(db.pool())
        .await
        .context("cleanup instances")?;
    sqlx::query("DELETE FROM workflow_versions WHERE workflow_name = $1")
        .bind(workflow_name)
        .execute(db.pool())
        .await
        .context("cleanup workflow versions")?;
    Ok(())
}
