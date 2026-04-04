use std::{
    num::{NonZeroU128, NonZeroUsize},
    path::PathBuf,
};

use anyhow::{Result, bail};
use clap::Parser;
use serde::Serialize;
use waymark_secret_string::{SecretStr, SecretString};

const DEFAULT_DSN: &SecretStr =
    SecretStr::new("postgresql://waymark:waymark@127.0.0.1:5433/waymark");

#[derive(Parser, Debug, Clone, Serialize)]
#[command(
    name = "soak-harness",
    about = "Run a long-lived, timeout-capable Waymark soak workload with diagnostics"
)]
pub struct SoakArgs {
    #[arg(
        long,
        env = "WAYMARK_DATABASE_URL",
        default_value = DEFAULT_DSN.expose_secret()
    )]
    pub dsn: SecretString,

    #[arg(long, default_value_t = false)]
    pub skip_postgres_boot: bool,

    #[arg(long, default_value_t = false)]
    pub skip_worker_launch: bool,

    #[arg(long, default_value_t = false)]
    pub keep_existing_data: bool,

    #[arg(long, default_value = "tests.fixtures_actions.soak_actions")]
    pub user_module: String,

    #[arg(long, default_value_t = 8)]
    pub worker_count: usize,

    #[arg(long, default_value_t = 500)]
    pub concurrent_per_worker: usize,

    #[arg(long, default_value_t = 5000)]
    pub max_concurrent_instances: usize,

    #[arg(long, default_value_t = 100)]
    pub poll_interval_ms: u64,

    #[arg(long, default_value_t = 500)]
    pub persist_interval_ms: u64,

    #[arg(long, default_value_t = 5000)]
    pub profile_interval_ms: u64,

    #[arg(long, default_value_t = false)]
    pub disable_webapp: bool,

    #[arg(long, default_value = "0.0.0.0:24119")]
    pub webapp_addr: String,

    #[arg(long, default_value_t = 5)]
    pub startup_log_interval_secs: u64,

    #[arg(long, default_value_t = 20)]
    pub timeout_seconds: u32,

    #[arg(long, default_value_t = 10_000.try_into().unwrap())]
    pub queue_rate_per_minute: NonZeroU128,

    #[arg(long, default_value_t = 5.try_into().unwrap())]
    pub actions_per_workflow: NonZeroUsize,

    #[arg(long, default_value_t = 500)]
    pub queue_batch_size: usize,

    #[arg(long, default_value_t = 50_000)]
    pub target_ready_queue: i64,

    #[arg(long, default_value_t = 10_000.try_into().unwrap())]
    pub max_top_up_per_tick: NonZeroU128,

    #[arg(long, default_value_t = 10_000.try_into().unwrap())]
    pub max_queue_per_tick: NonZeroU128,

    #[arg(long, default_value_t = 10)]
    pub tick_seconds: u64,

    #[arg(long)]
    pub duration_hours: Option<f64>,

    #[arg(long, default_value_t = 10.0)]
    pub timeout_percent: f64,

    #[arg(long, default_value_t = 3.0)]
    pub failure_percent: f64,

    #[arg(long, default_value_t = 25.0)]
    pub slow_percent: f64,

    #[arg(long, default_value_t = 4096)]
    pub payload_bytes: i64,

    #[arg(long, default_value_t = 1000)]
    pub issue_min_ready_queue: i64,

    #[arg(long, default_value_t = 0.01)]
    pub issue_actions_per_sec_threshold: f64,

    #[arg(long, default_value_t = 12)]
    pub issue_consecutive_samples: usize,

    #[arg(long, default_value_t = 90)]
    pub issue_last_action_stale_secs: i64,

    #[arg(long, default_value_t = 30)]
    pub issue_status_stale_secs: i64,

    #[arg(long, default_value_t = 20)]
    pub pg_stat_limit: i64,

    #[arg(long, default_value_t = 400)]
    pub max_diagnostic_tail_lines: usize,

    #[arg(long, default_value = "target/soak-runs")]
    pub diagnostic_dir: PathBuf,

    #[arg(long)]
    pub seed: Option<u64>,
}

pub fn validate_args(args: &SoakArgs) -> Result<()> {
    if args.queue_batch_size == 0 {
        bail!("--queue-batch-size must be at least 1");
    }
    if args.tick_seconds == 0 {
        bail!("--tick-seconds must be at least 1");
    }
    if args.issue_consecutive_samples == 0 {
        bail!("--issue-consecutive-samples must be at least 1");
    }
    if args.timeout_percent < 0.0 || args.failure_percent < 0.0 || args.slow_percent < 0.0 {
        bail!("workload percentages cannot be negative");
    }
    let used = args.timeout_percent + args.failure_percent + args.slow_percent;
    if used > 100.0 {
        bail!(
            "timeout/failure/slow percentages sum to {:.2}, must be <= 100",
            used
        );
    }
    Ok(())
}
