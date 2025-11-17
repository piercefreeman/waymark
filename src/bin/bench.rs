use std::{env, process, time::Duration};

use anyhow::{Result, anyhow};
use carabiner::{BenchmarkHarness, Database, HarnessConfig, PythonWorkerConfig};
use tracing::info;

#[derive(Debug, Clone)]
struct Options {
    total_messages: usize,
    payload_size: usize,
    concurrency: usize,
    database_url: String,
    partition_id: i32,
    log_interval_secs: Option<u64>,
    worker_count: usize,
    user_module: String,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            payload_size: 4096,
            concurrency: 32,
            database_url: default_database_url(),
            partition_id: 0,
            log_interval_secs: Some(30),
            worker_count: 1,
            user_module: PythonWorkerConfig::default().user_module,
        }
    }
}

impl Options {
    fn parse() -> Result<Self> {
        let mut opts = Options::default();
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--messages" | "-m" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--messages requires a value"))?;
                    opts.total_messages = value.parse()?;
                }
                "--payload" | "-p" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--payload requires a value"))?;
                    opts.payload_size = value.parse()?;
                }
                "--concurrency" | "-c" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--concurrency requires a value"))?;
                    opts.concurrency = value.parse()?;
                }
                "--help" | "-h" => {
                    print_usage();
                    process::exit(0);
                }
                "--database-url" | "-d" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--database-url requires a value"))?;
                    opts.database_url = value;
                }
                "--partition" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--partition requires a value"))?;
                    opts.partition_id = value.parse()?;
                }
                "--log-interval" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--log-interval requires seconds value"))?;
                    let secs: u64 = value.parse()?;
                    opts.log_interval_secs = if secs == 0 { None } else { Some(secs) };
                }
                "--workers" | "-w" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--workers requires a value"))?;
                    opts.worker_count = value.parse()?;
                }
                "--user-module" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--user-module requires a value"))?;
                    opts.user_module = value;
                }
                other => {
                    return Err(anyhow!("unknown argument: {other}"));
                }
            }
        }

        Ok(opts)
    }
}

fn print_usage() {
    println!(
        "Usage: cargo run --bin bench -- [--messages N] [--payload BYTES] [--concurrency N] [--database-url URL] [--partition ID] [--log-interval seconds] [--workers N] [--user-module MODULE]"
    );
}

fn default_database_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://mountaineer:mountaineer@localhost:5433/mountaineer_daemons".to_string()
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let options = Options::parse()?;

    info!(?options, "starting benchmark");
    let database = Database::connect(&options.database_url).await?;
    let mut worker_config = PythonWorkerConfig::default();
    worker_config.user_module = options.user_module.clone();
    let harness = BenchmarkHarness::new(worker_config, options.worker_count, database).await?;

    let config = HarnessConfig {
        total_messages: options.total_messages,
        in_flight: options.concurrency,
        payload_size: options.payload_size,
        partition_id: options.partition_id,
        progress_interval: options.log_interval_secs.map(Duration::from_secs),
    };

    let summary = harness.run(&config).await?;
    summary.log();
    println!(
        "Processed {} messages in {:.2?} - {:.0} msg/s (p95 {:.3} ms)",
        summary.total_messages,
        summary.elapsed,
        summary.throughput_per_sec,
        summary.p95_round_trip_ms,
    );

    harness.shutdown().await?;
    Ok(())
}
