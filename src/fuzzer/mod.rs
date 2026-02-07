//! Randomized workflow fuzzing harness.

mod generator;
mod harness;

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use clap::Parser;
use proptest::test_runner::{Config, RngSeed, TestRunner};

/// CLI args for the fuzz harness.
#[derive(Parser, Debug, Clone)]
#[command(name = "rappel-fuzz", about = "Run randomized IR workflow fuzz cases.")]
pub struct FuzzArgs {
    /// Number of random workflow cases to execute.
    #[arg(long, default_value_t = 50)]
    pub cases: usize,
    /// Maximum number of generated steps per workflow.
    #[arg(long, default_value_t = 8)]
    pub max_steps: usize,
    /// Optional deterministic seed.
    #[arg(long)]
    pub seed: Option<u64>,
}

/// Run randomized workflow fuzz cases end-to-end through parse, DAG conversion, and runloop.
pub async fn run(args: FuzzArgs) -> Result<()> {
    let seed = args.seed.unwrap_or_else(seed_from_clock);
    let total = args.cases.max(1);
    let mut config = Config {
        failure_persistence: None,
        ..Config::default()
    };
    config.rng_seed = RngSeed::Fixed(seed);
    let mut runner = TestRunner::new(config);

    println!(
        "Starting fuzz run: cases={total} max_steps={} seed={seed}",
        args.max_steps
    );

    for idx in 0..total {
        let case = generator::generate_case(&mut runner, args.max_steps.max(1), idx)
            .map_err(|err| anyhow!("failed to generate case {idx}: {err}"))?;
        harness::run_case(idx, &case).await?;
    }

    println!("Fuzz run completed successfully: cases={total} seed={seed}");
    Ok(())
}

fn seed_from_clock() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_nanos() as u64
}
