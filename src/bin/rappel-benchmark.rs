//! End-to-end benchmark for Rappel.
//!
//! This benchmark:
//! 1. Connects to a real PostgreSQL database
//! 2. Spins up N HostCoordinators (simulating multiple hosts)
//! 3. Queues workflow instances directly to the database
//! 4. Measures end-to-end throughput including DB + gRPC + worker execution

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use rappel::{
    coordinator::HostConfig,
    db::{Database, InstanceStatus},
    HostCoordinator,
};

#[derive(Parser, Debug)]
#[command(name = "rappel-benchmark", about = "End-to-end Rappel benchmark")]
struct Args {
    /// Output results as JSON
    #[arg(long, default_value = "false")]
    json: bool,

    /// Number of host coordinators to run (simulates multi-host cluster)
    #[arg(long, default_value = "1")]
    hosts: usize,

    /// Number of workflow instances to queue
    #[arg(long, default_value = "100")]
    workflows: usize,

    /// Number of actions per workflow
    #[arg(long, default_value = "10")]
    actions_per_workflow: usize,

    /// Action workers per host
    #[arg(long, default_value = "4")]
    action_workers: usize,

    /// Instance workers per host
    #[arg(long, default_value = "2")]
    instance_workers: usize,

    /// Max concurrent actions per worker
    #[arg(long, default_value = "10")]
    max_concurrent_actions: usize,

    /// Max concurrent instances per worker
    #[arg(long, default_value = "5")]
    max_concurrent_instances: usize,

    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Python module containing benchmark workflow
    #[arg(long, default_value = "benchmark_workflow")]
    user_module: String,

    /// Timeout for benchmark completion (seconds)
    #[arg(long, default_value = "30")]
    timeout_secs: u64,

    /// Python directory containing pyproject.toml
    #[arg(long)]
    python_dir: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BenchmarkResults {
    hosts: usize,
    workflows: usize,
    actions_per_workflow: usize,
    total_actions: usize,
    action_workers_per_host: usize,
    instance_workers_per_host: usize,
    elapsed_secs: f64,
    workflows_per_sec: f64,
    actions_per_sec: f64,
    completed: usize,
    failed: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging (quiet for JSON output)
    if !args.json {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    let separator = "=".repeat(70);

    if !args.json {
        println!("{}", separator);
        println!("Rappel End-to-End Benchmark");
        println!("{}", separator);
        println!();
        println!("Configuration:");
        println!("  Hosts: {}", args.hosts);
        println!("  Workflows: {}", args.workflows);
        println!("  Actions per workflow: {}", args.actions_per_workflow);
        println!("  Action workers per host: {}", args.action_workers);
        println!("  Instance workers per host: {}", args.instance_workers);
        println!("  Max concurrent actions/worker: {}", args.max_concurrent_actions);
        println!("  Max concurrent instances/worker: {}", args.max_concurrent_instances);
        println!();
    }

    // Connect to database
    let db = Arc::new(Database::connect(&args.database_url).await?);
    info!("Connected to database");

    // Run migrations
    db.migrate().await?;

    // Clean up any stale data from previous runs
    cleanup_database(&db).await?;

    // Start host coordinators
    let mut coordinators = Vec::with_capacity(args.hosts);
    let base_port = 24200;

    for i in 0..args.hosts {
        let config = HostConfig {
            http_addr: format!("127.0.0.1:{}", base_port + i * 10).parse()?,
            action_grpc_addr: format!("127.0.0.1:{}", base_port + i * 10 + 1).parse()?,
            instance_grpc_addr: format!("127.0.0.1:{}", base_port + i * 10 + 2).parse()?,
            action_worker_count: args.action_workers,
            instance_worker_count: args.instance_workers,
            max_concurrent_per_action_worker: args.max_concurrent_actions,
            max_concurrent_per_instance_worker: args.max_concurrent_instances,
            user_modules: vec![args.user_module.clone()],
            poll_interval_ms: 10, // Fast polling for benchmark
            python_dir: args.python_dir.clone(),
        };

        let mut coordinator = HostCoordinator::new(config, db.clone());
        coordinator.start().await?;
        coordinators.push(coordinator);

        if !args.json {
            println!("Started host coordinator {}", i + 1);
        }
    }

    // Wait for workers to connect
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Print capacity stats
    if !args.json {
        println!();
        println!("Cluster capacity:");
        for (i, coord) in coordinators.iter().enumerate() {
            let stats = coord.stats().await;
            println!(
                "  Host {}: {} action workers, {} instance workers, {} action slots, {} instance slots",
                i + 1,
                stats.action_workers_connected,
                stats.instance_workers_connected,
                stats.action_slots_available,
                stats.instance_slots_available
            );
        }
        println!();
    }

    // Queue workflow instances
    if !args.json {
        println!("Queueing {} workflow instances...", args.workflows);
    }

    let queue_start = Instant::now();
    let mut instance_ids = Vec::with_capacity(args.workflows);

    for _i in 0..args.workflows {
        let initial_args = serde_json::json!({
            "count": args.actions_per_workflow,
        });

        let instance_id = db
            .create_instance("BenchmarkWorkflow", &args.user_module, initial_args)
            .await?;
        instance_ids.push(instance_id);
    }

    let queue_time = queue_start.elapsed();
    if !args.json {
        println!(
            "Queued {} workflows in {:.3}s ({:.0} queues/sec)",
            args.workflows,
            queue_time.as_secs_f64(),
            args.workflows as f64 / queue_time.as_secs_f64()
        );
        println!();
        println!("Waiting for completion...");
    }

    // Wait for all workflows to complete
    let execution_start = Instant::now();
    let timeout = Duration::from_secs(args.timeout_secs);
    let poll_interval = Duration::from_millis(100);

    let mut completed = 0usize;
    let mut failed = 0usize;

    loop {
        if execution_start.elapsed() > timeout {
            warn!("Benchmark timed out after {} seconds", args.timeout_secs);
            break;
        }

        // Count completed and failed instances
        completed = 0;
        failed = 0;

        for &id in &instance_ids {
            if let Some(instance) = db.get_instance(id).await? {
                match instance.status {
                    InstanceStatus::Completed => completed += 1,
                    InstanceStatus::Failed => failed += 1,
                    _ => {}
                }
            }
        }

        if completed + failed == args.workflows {
            break;
        }

        if !args.json {
            print!(
                "\r  Progress: {}/{} completed, {} failed",
                completed, args.workflows, failed
            );
            use std::io::Write;
            std::io::stdout().flush().ok();
        }

        tokio::time::sleep(poll_interval).await;
    }

    let execution_time = execution_start.elapsed();

    if !args.json {
        println!();
        println!();
    }

    // Calculate results
    let total_actions = args.workflows * args.actions_per_workflow;
    let results = BenchmarkResults {
        hosts: args.hosts,
        workflows: args.workflows,
        actions_per_workflow: args.actions_per_workflow,
        total_actions,
        action_workers_per_host: args.action_workers,
        instance_workers_per_host: args.instance_workers,
        elapsed_secs: execution_time.as_secs_f64(),
        workflows_per_sec: completed as f64 / execution_time.as_secs_f64(),
        actions_per_sec: (completed * args.actions_per_workflow) as f64
            / execution_time.as_secs_f64(),
        completed,
        failed,
    };

    // Output results
    if args.json {
        println!("{}", serde_json::to_string_pretty(&results)?);
    } else {
        println!("{}", separator);
        println!("Results:");
        println!("{}", separator);
        println!("  Workflows completed: {}/{}", completed, args.workflows);
        println!("  Workflows failed: {}", failed);
        println!("  Total actions: {}", completed * args.actions_per_workflow);
        println!("  Execution time: {:.3}s", execution_time.as_secs_f64());
        println!("  Throughput: {:.1} workflows/sec", results.workflows_per_sec);
        println!("  Throughput: {:.1} actions/sec", results.actions_per_sec);
        println!();

        // Print efficiency metrics
        let total_action_workers = args.hosts * args.action_workers;
        let total_action_capacity =
            total_action_workers * args.max_concurrent_actions;
        let theoretical_max = total_action_capacity as f64 * (1000.0 / 10.0); // Assuming 10ms poll
        println!("  Total action workers: {}", total_action_workers);
        println!("  Total action capacity: {} concurrent", total_action_capacity);
        println!(
            "  Efficiency: {:.1}% of theoretical max",
            (results.actions_per_sec / theoretical_max) * 100.0
        );
    }

    // Stop coordinators
    for mut coord in coordinators {
        coord.stop().await;
    }

    Ok(())
}

/// Clean up database from previous benchmark runs.
async fn cleanup_database(db: &Database) -> Result<()> {
    // Delete all workflow instances and actions (CASCADE will handle actions)
    sqlx::query("DELETE FROM workflow_instances WHERE workflow_name = 'BenchmarkWorkflow'")
        .execute(db.pool())
        .await?;

    Ok(())
}
