use std::{env, process};

use anyhow::{Result, anyhow};
use carabiner::{BenchmarkHarness, HarnessConfig, PythonWorkerConfig};
use tracing::info;

#[derive(Debug, Clone)]
struct Options {
    total_messages: usize,
    payload_size: usize,
    concurrency: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            payload_size: 4096,
            concurrency: 32,
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
                other => {
                    return Err(anyhow!("unknown argument: {other}"));
                }
            }
        }

        Ok(opts)
    }
}

fn print_usage() {
    println!("Usage: cargo run --bin bench -- [--messages N] [--payload BYTES] [--concurrency N]");
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let options = Options::parse()?;

    info!(?options, "starting benchmark");
    let harness = BenchmarkHarness::new(PythonWorkerConfig::default()).await?;

    let config = HarnessConfig {
        total_messages: options.total_messages,
        in_flight: options.concurrency,
        payload_size: options.payload_size,
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
