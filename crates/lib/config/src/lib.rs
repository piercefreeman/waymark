//! Shared configuration helpers for Waymark binaries.

mod parse;

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use waymark_garbage_collector_config::GarbageCollectorConfig;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_scheduler_config::SchedulerConfig;
use waymark_secret_string::SecretString;

#[derive(Debug, Clone)]
pub enum ActionWorkerConfig {
    Python {
        user_modules: Vec<String>,
    },
    JavaScript {
        action_bundle: PathBuf,
        command: PathBuf,
    },
}

impl ActionWorkerConfig {
    pub fn runtime(&self) -> waymark_action_core::ActionRuntime {
        match self {
            Self::Python { .. } => waymark_action_core::ActionRuntime::Python,
            Self::JavaScript { .. } => waymark_action_core::ActionRuntime::JavaScript,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub database_url: SecretString,
    pub action_worker: ActionWorkerConfig,
    pub worker_grpc_addr: SocketAddr,
    pub worker_count: NonZeroUsize,
    pub concurrent_per_worker: NonZeroUsize,
    pub max_action_lifecycle: Option<NonZeroU64>,
    pub poll_interval: Option<NonZeroDuration>,
    pub max_concurrent_instances: NonZeroUsize,
    pub executor_shards: NonZeroUsize,
    pub instance_done_batch_size: Option<NonZeroUsize>,
    pub persistence_interval: Option<NonZeroDuration>,
    pub lock_ttl: NonZeroDuration,
    pub lock_heartbeat: NonZeroDuration,
    pub action_effect_reconciler_lock_ttl: NonZeroDuration,
    pub action_effect_reconciler_lock_heartbeat: NonZeroDuration,
    pub evict_sleep_threshold: NonZeroDuration,
    pub expired_lock_reclaimer_interval: NonZeroDuration,
    pub expired_lock_reclaimer_batch_size: NonZeroUsize,
    pub scheduler: SchedulerConfig,
    pub garbage_collector: GarbageCollectorConfig,
    pub webapp: waymark_webapp_config::WebappConfig,
    pub profile_interval: NonZeroDuration,
    pub vm_retention: NonZeroDuration,
    pub vm_sweep_interval: NonZeroDuration,
    pub executable_retention: NonZeroDuration,
    pub executable_sweep_interval: NonZeroDuration,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        use self::parse::*;

        let database_url = envfury::must("WAYMARK_DATABASE_URL")?;
        let action_runtime: waymark_action_core::ActionRuntime =
            envfury::must("WAYMARK_ACTION_RUNTIME")?;

        let worker_grpc_addr = envfury::or_parse("WAYMARK_WORKER_GRPC_ADDR", "127.0.0.1:24118")?;

        let worker_count = envfury::or_else("WAYMARK_WORKER_COUNT", default_worker_count)?;

        let concurrent_per_worker = envfury::or_parse("WAYMARK_CONCURRENT_PER_WORKER", "10")?;

        let action_worker = match action_runtime {
            waymark_action_core::ActionRuntime::Python => {
                let CommaSeparated(user_modules) =
                    envfury::or_parse("WAYMARK_PYTHON_USER_MODULE", "")?;
                ActionWorkerConfig::Python { user_modules }
            }
            waymark_action_core::ActionRuntime::JavaScript => ActionWorkerConfig::JavaScript {
                action_bundle: envfury::must("WAYMARK_JAVASCRIPT_ACTION_BUNDLE")?,
                command: envfury::or_parse(
                    "WAYMARK_JAVASCRIPT_WORKER_COMMAND",
                    "waymark-worker-node",
                )?,
            },
        };

        let max_action_lifecycle = envfury::maybe("WAYMARK_MAX_ACTION_LIFECYCLE")?;

        let FromMillis(poll_interval) = envfury::or_parse("WAYMARK_POLL_INTERVAL_MS", "100")?;
        let poll_interval = Some(poll_interval);

        let max_concurrent_instances =
            envfury::or_parse("WAYMARK_MAX_CONCURRENT_INSTANCES", "500")?;

        let executor_shards = envfury::or_else("WAYMARK_EXECUTOR_SHARDS", default_executor_shards)?;

        let instance_done_batch_size = envfury::maybe("WAYMARK_INSTANCE_DONE_BATCH_SIZE")?;

        let FromMillis(persistence_interval) =
            envfury::or_parse("WAYMARK_PERSIST_INTERVAL_MS", "500")?;
        let persistence_interval = Some(persistence_interval);

        let FromMillis(lock_ttl) = envfury::or_parse("WAYMARK_LOCK_TTL_MS", "15000")?;

        let FromMillis(lock_heartbeat) = envfury::or_parse("WAYMARK_LOCK_HEARTBEAT_MS", "5000")?;

        let FromMillis(action_effect_reconciler_lock_ttl) =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_TTL_MS", "15000")?;

        let FromMillis(action_effect_reconciler_lock_heartbeat) =
            envfury::or_parse("WAYMARK_ACTION_EFFECT_RECONCILER_LOCK_HEARTBEAT_MS", "5000")?;

        let FromMillis(evict_sleep_threshold) =
            envfury::or_parse("WAYMARK_EVICT_SLEEP_THRESHOLD_MS", "10000")?;

        let FromMillisMin::<_, 1>(expired_lock_reclaimer_interval) =
            envfury::or_parse("WAYMARK_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS", "15000")?;

        let expired_lock_reclaimer_batch_size: NonZeroUsize =
            envfury::or_parse("WAYMARK_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE", "1000")?;

        let scheduler = {
            let FromMillis(poll_interval) =
                envfury::or_parse("WAYMARK_SCHEDULER_POLL_INTERVAL_MS", "1000")?;
            let batch_size = envfury::or_parse("WAYMARK_SCHEDULER_BATCH_SIZE", "100")?;

            SchedulerConfig {
                poll_interval,
                batch_size,
            }
        };

        let garbage_collector = {
            let FromMillisMin::<_, 1>(interval) = envfury::or_into(
                "WAYMARK_GARBAGE_COLLECTOR_INTERVAL_MS",
                Duration::from_millis(5 * 60 * 1000),
            )?;

            let batch_size: NonZeroUsize =
                envfury::or_parse("WAYMARK_GARBAGE_COLLECTOR_BATCH_SIZE", "100")?;

            let retention_hours: NonZeroU64 =
                envfury::or_parse("WAYMARK_GARBAGE_COLLECTOR_RETENTION_HOURS", "24")?;
            let retention = Duration::from_secs(retention_hours.get() * 60 * 60);

            GarbageCollectorConfig {
                interval,
                batch_size: batch_size.get(),
                retention,
            }
        };

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
            action_worker,
            worker_grpc_addr,
            worker_count,
            concurrent_per_worker,
            max_action_lifecycle,
            poll_interval,
            max_concurrent_instances,
            executor_shards,
            instance_done_batch_size,
            persistence_interval,
            lock_ttl,
            lock_heartbeat,
            action_effect_reconciler_lock_ttl,
            action_effect_reconciler_lock_heartbeat,
            evict_sleep_threshold,
            expired_lock_reclaimer_interval,
            expired_lock_reclaimer_batch_size,
            scheduler,
            garbage_collector,
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

fn default_executor_shards() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(1.try_into().unwrap())
}
