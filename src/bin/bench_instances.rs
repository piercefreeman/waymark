use std::{env, process, time::Duration};

use anyhow::{Result, anyhow};
use rappel::{
    AppConfig, Database, PythonWorkerConfig, WorkflowBenchmarkConfig, WorkflowBenchmarkHarness,
};
use tracing::info;

#[derive(Debug, Clone)]
struct Options {
    instance_count: usize,
    batch_size: usize,
    payload_size: usize,
    concurrency: usize,
    worker_count: usize,
    log_interval_secs: Option<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            instance_count: 100,
            batch_size: 4,
            payload_size: 1024,
            concurrency: 32,
            worker_count: 1,
            log_interval_secs: Some(30),
        }
    }
}

impl Options {
    fn parse() -> Result<Self> {
        let mut opts = Options::default();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--instances" | "-n" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--instances requires a value"))?;
                    opts.instance_count = value.parse()?;
                }
                "--batch-size" | "-b" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--batch-size requires a value"))?;
                    opts.batch_size = value.parse()?;
                }
                "--payload-size" | "-p" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--payload-size requires a value"))?;
                    opts.payload_size = value.parse()?;
                }
                "--concurrency" | "-c" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--concurrency requires a value"))?;
                    opts.concurrency = value.parse()?;
                }
                "--workers" | "-w" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--workers requires a value"))?;
                    opts.worker_count = value.parse()?;
                }
                "--log-interval" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--log-interval requires a value"))?;
                    let secs: u64 = value.parse()?;
                    opts.log_interval_secs = if secs == 0 { None } else { Some(secs) };
                }
                "--help" | "-h" => {
                    print_usage();
                    process::exit(0);
                }
                other => return Err(anyhow!("unknown argument: {other}")),
            }
        }
        Ok(opts)
    }
}

fn print_usage() {
    println!(
        "Usage: cargo run --bin bench_instances -- [--instances N] [--batch-size N] [--payload-size BYTES] [--concurrency N] [--workers N] [--log-interval SECONDS]"
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let options = Options::parse()?;
    let app_config = AppConfig::load()?;
    info!(?options, "starting workflow benchmark");
    let database = Database::connect(&app_config.database_url).await?;
    let worker_config = PythonWorkerConfig::default();
    let harness =
        WorkflowBenchmarkHarness::new(database, options.worker_count, worker_config).await?;

    let config = WorkflowBenchmarkConfig {
        instance_count: options.instance_count,
        in_flight: options.concurrency,
        batch_size: options.batch_size,
        payload_size: options.payload_size,
        progress_interval: options.log_interval_secs.map(Duration::from_secs),
    };

    let summary = harness.run(&config).await?;
    summary.log();
    let actions = options.instance_count * harness.actions_per_instance();
    let workflow_rate = options.instance_count as f64 / summary.elapsed.as_secs_f64().max(1e-9);
    println!(
        "Processed {} workflows ({} actions) in {:.2?} - {:.0} actions/s ({:.2} workflows/s)",
        options.instance_count, actions, summary.elapsed, summary.throughput_per_sec, workflow_rate,
    );

    harness.shutdown().await?;
    Ok(())
}
