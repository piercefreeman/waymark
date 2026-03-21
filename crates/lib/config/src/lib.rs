//! Shared configuration helpers for Waymark binaries.

mod parse;

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use waymark_garbage_collector_config::GarbageCollectorConfig;
use waymark_scheduler_config::SchedulerConfig;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub database_url: String,
    pub worker_grpc_addr: SocketAddr,
    pub worker_count: usize,
    pub concurrent_per_worker: usize,
    pub user_modules: Vec<String>,
    pub max_action_lifecycle: Option<u64>,
    pub poll_interval: Duration,
    pub max_concurrent_instances: usize,
    pub executor_shards: usize,
    pub instance_done_batch_size: Option<usize>,
    pub persistence_interval: Duration,
    pub lock_ttl: Duration,
    pub lock_heartbeat: Duration,
    pub evict_sleep_threshold: Duration,
    pub expired_lock_reclaimer_interval: Duration,
    pub expired_lock_reclaimer_batch_size: usize,
    pub scheduler: SchedulerConfig,
    pub garbage_collector: GarbageCollectorConfig,
    pub webapp: waymark_webapp_config::WebappConfig,
    pub profile_interval: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        use self::parse::*;

        let database_url = envfury::must("WAYMARK_DATABASE_URL")?;

        let worker_grpc_addr = envfury::or_parse("WAYMARK_WORKER_GRPC_ADDR", "127.0.0.1:24118")?;

        let worker_count = envfury::or_else("WAYMARK_WORKER_COUNT", default_worker_count)?;

        let concurrent_per_worker = envfury::or("WAYMARK_CONCURRENT_PER_WORKER", 10)?;

        let CommaSeparated(user_modules) = envfury::or_parse("WAYMARK_USER_MODULE", "")?;

        let max_action_lifecycle = envfury::maybe("WAYMARK_MAX_ACTION_LIFECYCLE")?;

        let FromMillis(poll_interval) = envfury::or_parse("WAYMARK_POLL_INTERVAL_MS", "100")?;

        let max_concurrent_instances =
            envfury::or_parse("WAYMARK_MAX_CONCURRENT_INSTANCES", "500")?;

        let executor_shards = envfury::or_else("WAYMARK_EXECUTOR_SHARDS", default_executor_shards)?;

        let instance_done_batch_size = envfury::maybe("WAYMARK_INSTANCE_DONE_BATCH_SIZE")?;

        let FromMillis(persistence_interval) =
            envfury::or_parse("WAYMARK_PERSIST_INTERVAL_MS", "500")?;

        let FromMillis(lock_ttl) = envfury::or_parse("WAYMARK_LOCK_TTL_MS", "15000")?;

        let FromMillis(lock_heartbeat) = envfury::or_parse("WAYMARK_LOCK_HEARTBEAT_MS", "5000")?;

        let FromMillis(evict_sleep_threshold) =
            envfury::or_parse("WAYMARK_EVICT_SLEEP_THRESHOLD_MS", "10000")?;

        let FromMillisMin::<_, 1>(expired_lock_reclaimer_interval) =
            envfury::or_parse("WAYMARK_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS", "15000")?;

        let expired_lock_reclaimer_batch_size: NonZeroUsize =
            envfury::or_parse("WAYMARK_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE", "1000")?;

        let scheduler = {
            let FromMillis(poll_interval) =
                envfury::or_parse("WAYMARK_SCHEDULER_POLL_INTERVAL_MS", "1000")?;
            let batch_size = envfury::or_parse("WAYMARK_SCHEDULER_POLL_INTERVAL_MS", "100")?;

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

        Ok(Self {
            database_url,
            worker_grpc_addr,
            worker_count: worker_count.get(),
            concurrent_per_worker,
            user_modules,
            max_action_lifecycle,
            poll_interval,
            max_concurrent_instances,
            executor_shards: executor_shards.get(),
            instance_done_batch_size,
            persistence_interval,
            lock_ttl,
            lock_heartbeat,
            evict_sleep_threshold,
            expired_lock_reclaimer_interval,
            expired_lock_reclaimer_batch_size: expired_lock_reclaimer_batch_size.get(),
            scheduler,
            garbage_collector,
            webapp,
            profile_interval,
        })
    }
}

fn default_worker_count() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(1.try_into().unwrap())
}

fn default_executor_shards() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(1.try_into().unwrap())
}
