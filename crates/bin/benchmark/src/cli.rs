//! Command-line interface and environment knobs for the benchmark.

use std::env;
use std::num::NonZeroUsize;

use clap::Parser;
use waymark_secret_string::{SecretStr, SecretString};
use waymark_support_integration::LOCAL_POSTGRES_DSN;

const DEFAULT_DSN: &SecretStr = LOCAL_POSTGRES_DSN;
const DEFAULT_MAX_PINNED: NonZeroUsize = NonZeroUsize::new(500).unwrap();

#[derive(Parser, Debug)]
#[command(
    name = "waymark-benchmark",
    about = "Benchmark mixed IR workloads against Postgres."
)]
pub struct BenchmarkArgs {
    #[arg(long, default_value_t = 10_000.try_into().unwrap())]
    pub count: NonZeroUsize,
    #[arg(long, default_value_t = 5)]
    pub base: i64,
    #[arg(long, default_value = DEFAULT_DSN.expose_secret())]
    pub dsn: SecretString,
    #[arg(long, default_value_t = false)]
    pub observe: bool,
    #[arg(long, num_args = 0..=1, default_missing_value = "target/benchmark-trace.json")]
    pub trace: Option<String>,
}

pub fn benchmark_max_pinned() -> NonZeroUsize {
    env::var("WAYMARK_MAX_CONCURRENT_INSTANCES")
        .ok()
        .and_then(|value| value.trim().parse::<NonZeroUsize>().ok())
        .unwrap_or(DEFAULT_MAX_PINNED)
}
