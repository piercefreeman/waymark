//! Shared configuration helpers for Waymark binaries.

mod parse;

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretString;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub database_url: SecretString,
    pub worker_grpc_addr: SocketAddr,
    pub worker_count: NonZeroUsize,
    pub concurrent_per_worker: NonZeroUsize,
    pub user_modules: Vec<String>,
    pub max_action_lifecycle: Option<NonZeroU64>,
    pub max_concurrent_instances: NonZeroUsize,
    pub lock_ttl: NonZeroDuration,
    pub lock_heartbeat: NonZeroDuration,
    pub pinning_fencing_margin: NonZeroDuration,
    pub workload_poll_rate_limit: NonZeroU32,
    pub snapshot_batch_max: NonZeroUsize,
    pub snapshot_batch_delay: NonZeroDuration,
    pub action_effect_reconciler_request_batch_max: NonZeroUsize,
    pub action_effect_reconciler_request_batch_delay: NonZeroDuration,
    pub workflow_completion_batch_max: NonZeroUsize,
    pub workflow_completion_batch_delay: NonZeroDuration,
    pub action_effect_reconciler_lock_batch_max: NonZeroUsize,
    pub action_effect_reconciler_lock_batch_delay: NonZeroDuration,
    pub action_effect_reconciler_lock_ttl: NonZeroDuration,
    pub action_effect_reconciler_lock_heartbeat: NonZeroDuration,
    pub sleep_poll_interval: NonZeroDuration,
    pub webapp: waymark_webapp_config::WebappConfig,
    pub profile_interval: NonZeroDuration,
    pub vm_retention: NonZeroDuration,
    pub vm_sweep_interval: NonZeroDuration,
    pub executable_retention: NonZeroDuration,
    pub executable_sweep_interval: NonZeroDuration,
}

/// Error returned when reading a [`WorkerConfig`] from the environment.
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// A required variable could not be read.
    #[error(transparent)]
    Must(#[from] envfury::Error<envfury::MustError<std::convert::Infallible>>),

    /// A socket-address variable (with a parsed default) could not be read.
    #[error(transparent)]
    SocketAddrOrDefault(#[from] envfury::Error<envfury::OrParseError<std::net::AddrParseError>>),

    /// An integer-backed variable (with a parsed default) could not be read.
    #[error(transparent)]
    IntOrDefault(#[from] envfury::Error<envfury::OrParseError<core::num::ParseIntError>>),

    /// A list variable (with a parsed default) could not be read.
    #[error(transparent)]
    ListOrDefault(#[from] envfury::Error<envfury::OrParseError<std::convert::Infallible>>),

    /// An integer-backed variable (without a parsed default) could not be read.
    #[error(transparent)]
    Int(#[from] envfury::Error<envfury::ValueError<core::num::ParseIntError>>),
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, FromEnvError> {
        use self::parse::*;

        let database_url = envfury::must("WAYMARK_DATABASE_URL")?;

        let worker_grpc_addr = envfury::or_parse("WAYMARK_WORKER_GRPC_ADDR", "127.0.0.1:24118")?;

        let worker_count = envfury::or_else("WAYMARK_WORKER_COUNT", default_worker_count)?;

        let concurrent_per_worker = envfury::or_parse("WAYMARK_CONCURRENT_PER_WORKER", "10")?;

        let CommaSeparated(user_modules) = envfury::or_parse("WAYMARK_USER_MODULE", "")?;

        let max_action_lifecycle = envfury::maybe("WAYMARK_MAX_ACTION_LIFECYCLE")?;

        let max_concurrent_instances =
            envfury::or_parse("WAYMARK_MAX_CONCURRENT_INSTANCES", "500")?;

        let FromMillis(lock_ttl) = envfury::or_parse("WAYMARK_LOCK_TTL_MS", "15000")?;

        let FromMillis(lock_heartbeat) = envfury::or_parse("WAYMARK_LOCK_HEARTBEAT_MS", "5000")?;

        let FromMillis(pinning_fencing_margin) =
            envfury::or_parse("WAYMARK_PINNING_FENCING_MARGIN_MS", "1000")?;

        let workload_poll_rate_limit =
            envfury::or_parse("WAYMARK_WORKLOAD_POLL_RATE_LIMIT", "1000")?;

        let snapshot_batch_max = envfury::or_parse("WAYMARK_SNAPSHOT_BATCH_MAX", "256")?;

        let FromMillis(snapshot_batch_delay) =
            envfury::or_parse("WAYMARK_SNAPSHOT_BATCH_DELAY_MS", "5")?;

        let action_effect_reconciler_request_batch_max =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_MAX", "256")?;

        let FromMillis(action_effect_reconciler_request_batch_delay) = envfury::or_parse(
            "WAYMARK_ACTION_EFFECT_RECONCILER_REQUEST_BATCH_DELAY_MS",
            "5",
        )?;

        let workflow_completion_batch_max =
            envfury::or_parse("WAYMARK_WORKFLOW_COMPLETION_BATCH_MAX", "256")?;

        let FromMillis(workflow_completion_batch_delay) =
            envfury::or_parse("WAYMARK_WORKFLOW_COMPLETION_BATCH_DELAY_MS", "5")?;

        let action_effect_reconciler_lock_batch_max =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_MAX", "256")?;

        let FromMillis(action_effect_reconciler_lock_batch_delay) =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_BATCH_DELAY_MS", "5")?;

        let FromMillis(action_effect_reconciler_lock_ttl) =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_TTL_MS", "15000")?;

        let FromMillis(action_effect_reconciler_lock_heartbeat) =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_HEARTBEAT_MS", "5000")?;

        let FromMillis(sleep_poll_interval) =
            envfury::or_parse("WAYMARK_SLEEP_POLL_INTERVAL_MS", "250")?;

        let webapp = waymark_webapp_config::WebappConfig::from_env();

        let FromMillisMin::<_, 1>(profile_interval) =
            envfury::or_parse("WAYMARK_RUNNER_PROFILE_INTERVAL_MS", "5000")?;

        let FromMillis(vm_retention) = envfury::or_parse("WAYMARK_VM_RETENTION_MS", "60000")?;

        let FromMillis(vm_sweep_interval) =
            envfury::or_parse("WAYMARK_VM_SWEEP_INTERVAL_MS", "10000")?;

        let FromMillis(executable_retention) =
            envfury::or_parse("WAYMARK_EXECUTABLE_RETENTION_MS", "300000")?;

        let FromMillis(executable_sweep_interval) =
            envfury::or_parse("WAYMARK_EXECUTABLE_SWEEP_INTERVAL_MS", "60000")?;

        Ok(Self {
            database_url,
            worker_grpc_addr,
            worker_count,
            concurrent_per_worker,
            user_modules,
            max_action_lifecycle,
            max_concurrent_instances,
            lock_ttl,
            lock_heartbeat,
            pinning_fencing_margin,
            workload_poll_rate_limit,
            snapshot_batch_max,
            snapshot_batch_delay,
            action_effect_reconciler_request_batch_max,
            action_effect_reconciler_request_batch_delay,
            workflow_completion_batch_max,
            workflow_completion_batch_delay,
            action_effect_reconciler_lock_batch_max,
            action_effect_reconciler_lock_batch_delay,
            action_effect_reconciler_lock_ttl,
            action_effect_reconciler_lock_heartbeat,
            sleep_poll_interval,
            webapp,
            profile_interval,
            vm_retention,
            vm_sweep_interval,
            executable_retention,
            executable_sweep_interval,
        })
    }
}

fn default_worker_count() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(1.try_into().unwrap())
}
