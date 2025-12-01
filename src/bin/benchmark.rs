use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use rappel::{
    AppConfig, BenchmarkHarness, BenchmarkSummary, Database, FanoutBenchmarkConfig,
    FanoutBenchmarkHarness, HarnessConfig, PythonWorkerConfig, StressBenchmarkConfig,
    StressBenchmarkHarness, WorkflowBenchmarkConfig, WorkflowBenchmarkHarness,
};
use serde::Serialize;
use tracing::info;

#[derive(Parser)]
#[command(name = "benchmark")]
#[command(about = "Rappel benchmark suite for testing throughput and performance")]
struct Cli {
    /// Output results as JSON to stdout
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

/// JSON output format for benchmark results
#[derive(Serialize)]
struct BenchmarkOutput {
    /// Total number of messages/actions processed
    total: usize,
    /// Elapsed time in seconds
    elapsed_s: f64,
    /// Throughput in messages/actions per second
    throughput: f64,
    /// Average round-trip latency in milliseconds
    avg_round_trip_ms: f64,
    /// P95 round-trip latency in milliseconds
    p95_round_trip_ms: f64,
}

impl From<&BenchmarkSummary> for BenchmarkOutput {
    fn from(summary: &BenchmarkSummary) -> Self {
        Self {
            total: summary.total_messages,
            elapsed_s: summary.elapsed.as_secs_f64(),
            throughput: summary.throughput_per_sec,
            avg_round_trip_ms: summary.avg_round_trip_ms,
            p95_round_trip_ms: summary.p95_round_trip_ms,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Raw action throughput benchmark - seeds actions directly to DB and measures dispatch speed
    Actions {
        /// Total number of actions to process
        #[arg(short = 'n', long, default_value = "10000")]
        messages: usize,

        /// Payload size in bytes
        #[arg(short, long, default_value = "4096")]
        payload: usize,

        /// Maximum in-flight actions per worker
        #[arg(short, long, default_value = "32")]
        concurrency: usize,

        /// Number of worker processes
        #[arg(short, long, default_value = "1")]
        workers: usize,

        /// Progress reporting interval in seconds (0 to disable)
        #[arg(long, default_value = "30")]
        log_interval: u64,

        /// Python user module for worker
        #[arg(long)]
        user_module: Option<String>,
    },

    /// Workflow instance benchmark - creates workflow instances and measures end-to-end throughput
    Instances {
        /// Number of workflow instances to create
        #[arg(short = 'n', long, default_value = "100")]
        instances: usize,

        /// Items per workflow batch
        #[arg(short, long, default_value = "4")]
        batch_size: usize,

        /// Payload size in bytes
        #[arg(short, long, default_value = "1024")]
        payload_size: usize,

        /// Maximum in-flight actions per worker
        #[arg(short, long, default_value = "32")]
        concurrency: usize,

        /// Number of worker processes
        #[arg(short, long, default_value = "1")]
        workers: usize,

        /// Progress reporting interval in seconds (0 to disable)
        #[arg(long, default_value = "30")]
        log_interval: u64,
    },

    /// Stress test benchmark - complex workflows designed to saturate CPU cores
    Stress {
        /// Number of workflow instances to create
        #[arg(short = 'n', long, default_value = "50")]
        instances: usize,

        /// Number of parallel actions in fan-out phase
        #[arg(short, long, default_value = "16")]
        fan_out: usize,

        /// Number of loop iterations (each with 3-action body)
        #[arg(short, long, default_value = "8")]
        loop_iterations: usize,

        /// CPU work iterations per action (controls intensity)
        #[arg(short = 'i', long, default_value = "1000")]
        work_intensity: usize,

        /// Payload size in bytes
        #[arg(short, long, default_value = "1024")]
        payload_size: usize,

        /// Maximum in-flight actions per worker
        #[arg(short, long, default_value = "64")]
        concurrency: usize,

        /// Number of worker processes
        #[arg(short, long, default_value = "4")]
        workers: usize,

        /// Progress reporting interval in seconds (0 to disable)
        #[arg(long, default_value = "15")]
        log_interval: u64,
    },

    /// Fan-out benchmark - simple parallel workflows with push-based scheduling
    Fanout {
        /// Number of workflow instances to create
        #[arg(short = 'n', long, default_value = "100")]
        instances: usize,

        /// Number of parallel actions in fan-out phase
        #[arg(short, long, default_value = "16")]
        fan_out: usize,

        /// CPU work iterations per action (controls intensity)
        #[arg(short = 'i', long, default_value = "1000")]
        work_intensity: usize,

        /// Payload size in bytes
        #[arg(short, long, default_value = "1024")]
        payload_size: usize,

        /// Maximum in-flight actions per worker
        #[arg(short, long, default_value = "64")]
        concurrency: usize,

        /// Number of worker processes
        #[arg(short, long, default_value = "4")]
        workers: usize,

        /// Progress reporting interval in seconds (0 to disable)
        #[arg(long, default_value = "15")]
        log_interval: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let app_config = AppConfig::load()?;

    // Determine worker count from command to size DB pool appropriately
    let worker_count = match &cli.command {
        Commands::Actions { workers, .. } => *workers,
        Commands::Instances { workers, .. } => *workers,
        Commands::Stress { workers, .. } => *workers,
        Commands::Fanout { workers, .. } => *workers,
    };

    // Scale DB pool: base of 10 + 5 per worker to handle concurrent completions
    // Each worker can generate completion batches that need DB connections
    let pool_size = (10 + worker_count * 5).min(100) as u32;
    let database = Database::connect_with_pool_size(&app_config.database_url, pool_size).await?;

    let json_output = cli.json;

    match cli.command {
        Commands::Actions {
            messages,
            payload,
            concurrency,
            workers,
            log_interval,
            user_module,
        } => {
            run_actions_benchmark(
                database,
                messages,
                payload,
                concurrency,
                workers,
                log_interval,
                user_module,
                json_output,
            )
            .await
        }
        Commands::Instances {
            instances,
            batch_size,
            payload_size,
            concurrency,
            workers,
            log_interval,
        } => {
            run_instances_benchmark(
                database,
                instances,
                batch_size,
                payload_size,
                concurrency,
                workers,
                log_interval,
                json_output,
            )
            .await
        }
        Commands::Stress {
            instances,
            fan_out,
            loop_iterations,
            work_intensity,
            payload_size,
            concurrency,
            workers,
            log_interval,
        } => {
            run_stress_benchmark(
                database,
                instances,
                fan_out,
                loop_iterations,
                work_intensity,
                payload_size,
                concurrency,
                workers,
                log_interval,
                json_output,
            )
            .await
        }
        Commands::Fanout {
            instances,
            fan_out,
            work_intensity,
            payload_size,
            concurrency,
            workers,
            log_interval,
        } => {
            run_fanout_benchmark(
                database,
                instances,
                fan_out,
                work_intensity,
                payload_size,
                concurrency,
                workers,
                log_interval,
                json_output,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_actions_benchmark(
    database: Database,
    messages: usize,
    payload: usize,
    concurrency: usize,
    workers: usize,
    log_interval: u64,
    user_module: Option<String>,
    json_output: bool,
) -> Result<()> {
    info!(
        messages,
        payload, concurrency, workers, "starting actions benchmark"
    );

    let worker_config = PythonWorkerConfig {
        user_module: user_module.unwrap_or_else(|| PythonWorkerConfig::default().user_module),
        ..PythonWorkerConfig::default()
    };
    let harness = BenchmarkHarness::new(worker_config, workers, database).await?;

    let config = HarnessConfig {
        total_messages: messages,
        in_flight: concurrency,
        payload_size: payload,
        progress_interval: if log_interval == 0 {
            None
        } else {
            Some(Duration::from_secs(log_interval))
        },
    };

    let summary = harness.run(&config).await?;

    if json_output {
        let output = BenchmarkOutput::from(&summary);
        println!("{}", serde_json::to_string(&output)?);
    } else {
        summary.log();
        println!(
            "\n=== Actions Benchmark Results ===\n\
             Total messages:        {}\n\
             Payload size:          {} bytes\n\
             Workers:               {}\n\
             Concurrency/worker:    {}\n\
             \n\
             Elapsed time:          {:.2?}\n\
             Throughput:            {:.0} msg/s\n\
             P95 round-trip:        {:.2}ms\n",
            summary.total_messages,
            payload,
            workers,
            concurrency,
            summary.elapsed,
            summary.throughput_per_sec,
            summary.p95_round_trip_ms,
        );
    }

    harness.shutdown().await
}

#[allow(clippy::too_many_arguments)]
async fn run_instances_benchmark(
    database: Database,
    instances: usize,
    batch_size: usize,
    payload_size: usize,
    concurrency: usize,
    workers: usize,
    log_interval: u64,
    json_output: bool,
) -> Result<()> {
    info!(
        instances,
        batch_size, payload_size, concurrency, workers, "starting instances benchmark"
    );

    let worker_config = PythonWorkerConfig::default();
    let harness = WorkflowBenchmarkHarness::new(database, workers, worker_config).await?;

    let config = WorkflowBenchmarkConfig {
        instance_count: instances,
        in_flight: concurrency,
        batch_size,
        payload_size,
        progress_interval: if log_interval == 0 {
            None
        } else {
            Some(Duration::from_secs(log_interval))
        },
    };

    let summary = harness.run(&config).await?;

    if json_output {
        let output = BenchmarkOutput::from(&summary);
        println!("{}", serde_json::to_string(&output)?);
    } else {
        summary.log();
        let actions = instances * harness.actions_per_instance();
        let workflow_rate = instances as f64 / summary.elapsed.as_secs_f64().max(1e-9);

        println!(
            "\n=== Instances Benchmark Results ===\n\
             Workflow instances:    {}\n\
             Actions per instance:  {}\n\
             Total actions:         {}\n\
             Workers:               {}\n\
             Concurrency/worker:    {}\n\
             \n\
             Elapsed time:          {:.2?}\n\
             Throughput:            {:.0} actions/s\n\
             Workflow rate:         {:.2} workflows/s\n\
             P95 round-trip:        {:.2}ms\n",
            instances,
            harness.actions_per_instance(),
            actions,
            workers,
            concurrency,
            summary.elapsed,
            summary.throughput_per_sec,
            workflow_rate,
            summary.p95_round_trip_ms,
        );
    }

    harness.shutdown().await
}

#[allow(clippy::too_many_arguments)]
async fn run_stress_benchmark(
    database: Database,
    instances: usize,
    fan_out: usize,
    loop_iterations: usize,
    work_intensity: usize,
    payload_size: usize,
    concurrency: usize,
    workers: usize,
    log_interval: u64,
    json_output: bool,
) -> Result<()> {
    let actions_per_instance = 1 + fan_out + (loop_iterations * 3) + 1 + 1;
    let total_actions = instances * actions_per_instance;

    info!(
        instances,
        fan_out,
        loop_iterations,
        work_intensity,
        actions_per_instance,
        total_actions,
        workers,
        concurrency,
        "starting stress benchmark"
    );

    let worker_config = PythonWorkerConfig::default();
    let harness = StressBenchmarkHarness::new(database, workers, worker_config).await?;

    let config = StressBenchmarkConfig {
        instance_count: instances,
        fan_out_factor: fan_out,
        loop_iterations,
        work_intensity,
        payload_size,
        in_flight: concurrency,
        progress_interval: if log_interval == 0 {
            None
        } else {
            Some(Duration::from_secs(log_interval))
        },
    };

    let summary = harness.run(&config).await?;

    if json_output {
        let output = BenchmarkOutput::from(&summary);
        println!("{}", serde_json::to_string(&output)?);
    } else {
        summary.log();
        let workflow_rate = instances as f64 / summary.elapsed.as_secs_f64().max(1e-9);

        println!(
            "\n=== Stress Benchmark Results ===\n\
             Workflow instances:    {}\n\
             Actions per instance:  {}\n\
             Total actions:         {}\n\
             Fan-out factor:        {}\n\
             Loop iterations:       {}\n\
             Work intensity:        {}\n\
             Workers:               {}\n\
             Concurrency/worker:    {}\n\
             \n\
             Elapsed time:          {:.2?}\n\
             Throughput:            {:.0} actions/s\n\
             Workflow rate:         {:.2} workflows/s\n\
             P95 round-trip:        {:.2}ms\n",
            instances,
            actions_per_instance,
            total_actions,
            fan_out,
            loop_iterations,
            work_intensity,
            workers,
            concurrency,
            summary.elapsed,
            summary.throughput_per_sec,
            workflow_rate,
            summary.p95_round_trip_ms,
        );
    }

    harness.shutdown().await
}

#[allow(clippy::too_many_arguments)]
async fn run_fanout_benchmark(
    database: Database,
    instances: usize,
    fan_out: usize,
    work_intensity: usize,
    payload_size: usize,
    concurrency: usize,
    workers: usize,
    log_interval: u64,
    json_output: bool,
) -> Result<()> {
    // Actions per instance: 1 (setup) + fan_out (parallel) + 1 (finalize)
    let actions_per_instance = 1 + fan_out + 1;
    let total_actions = instances * actions_per_instance;

    info!(
        instances,
        fan_out,
        work_intensity,
        actions_per_instance,
        total_actions,
        workers,
        concurrency,
        "starting fanout benchmark"
    );

    let worker_config = PythonWorkerConfig::default();
    let harness = FanoutBenchmarkHarness::new(database, workers, worker_config).await?;

    let config = FanoutBenchmarkConfig {
        instance_count: instances,
        fan_out_factor: fan_out,
        work_intensity,
        payload_size,
        in_flight: concurrency,
        progress_interval: if log_interval == 0 {
            None
        } else {
            Some(Duration::from_secs(log_interval))
        },
    };

    let summary = harness.run(&config).await?;

    if json_output {
        let output = BenchmarkOutput::from(&summary);
        println!("{}", serde_json::to_string(&output)?);
    } else {
        summary.log();
        let workflow_rate = instances as f64 / summary.elapsed.as_secs_f64().max(1e-9);

        println!(
            "\n=== Fanout Benchmark Results ===\n\
             Workflow instances:    {}\n\
             Actions per instance:  {}\n\
             Total actions:         {}\n\
             Fan-out factor:        {}\n\
             Work intensity:        {}\n\
             Workers:               {}\n\
             Concurrency/worker:    {}\n\
             \n\
             Elapsed time:          {:.2?}\n\
             Throughput:            {:.0} actions/s\n\
             Workflow rate:         {:.2} workflows/s\n\
             P95 round-trip:        {:.2}ms\n",
            instances,
            actions_per_instance,
            total_actions,
            fan_out,
            work_intensity,
            workers,
            concurrency,
            summary.elapsed,
            summary.throughput_per_sec,
            workflow_rate,
            summary.p95_round_trip_ms,
        );
    }

    harness.shutdown().await
}
