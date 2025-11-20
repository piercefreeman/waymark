use std::{env, process, time::Duration};

use anyhow::{Result, anyhow};
use rappel::{AppConfig, BenchmarkHarness, Database, HarnessConfig, PythonWorkerConfig};
use tracing::info;

#[derive(Debug, Clone)]
struct Options {
    total_messages: usize,
    payload_size: usize,
    concurrency: usize,
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
        "Usage: cargo run --bin bench -- [--messages N] [--payload BYTES] [--concurrency N] [--log-interval seconds] [--workers N] [--user-module MODULE]"
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let options = Options::parse()?;
    let app_config = AppConfig::load()?;

    info!(?options, "starting benchmark");
    let database = Database::connect(&app_config.database_url).await?;
    let worker_config = PythonWorkerConfig {
        user_module: options.user_module.clone(),
        ..PythonWorkerConfig::default()
    };
    let harness = BenchmarkHarness::new(worker_config, options.worker_count, database).await?;

    let config = HarnessConfig {
        total_messages: options.total_messages,
        in_flight: options.concurrency,
        payload_size: options.payload_size,
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
