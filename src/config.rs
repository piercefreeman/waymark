//! Shared configuration helpers for Rappel binaries.

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::{SchedulerConfig, WebappConfig};

const DEFAULT_WORKER_GRPC_ADDR: &str = "127.0.0.1:24118";
const DEFAULT_WORKER_CONCURRENCY: usize = 10;
const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const DEFAULT_MAX_CONCURRENT_INSTANCES: usize = 500;
const DEFAULT_PERSIST_INTERVAL_MS: u64 = 500;
const DEFAULT_SCHEDULER_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_SCHEDULER_BATCH_SIZE: i32 = 100;
const DEFAULT_PROFILE_INTERVAL_MS: u64 = 5000;
const DEFAULT_EVICT_SLEEP_THRESHOLD_MS: u64 = 10_000;
const DEFAULT_LOCK_HEARTBEAT_MS: u64 = 5_000;
const DEFAULT_LOCK_TTL_MS: u64 = 15_000;
const DEFAULT_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS: u64 = 1_000;
const DEFAULT_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE: usize = 1_000;

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
    pub webapp: WebappConfig,
    pub profile_interval: Duration,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self> {
        let database_url =
            env::var("RAPPEL_DATABASE_URL").context("RAPPEL_DATABASE_URL must be set")?;

        let worker_grpc_addr = env::var("RAPPEL_WORKER_GRPC_ADDR")
            .unwrap_or_else(|_| DEFAULT_WORKER_GRPC_ADDR.to_string())
            .parse()
            .context("invalid RAPPEL_WORKER_GRPC_ADDR")?;

        let worker_count = env_usize("RAPPEL_WORKER_COUNT").unwrap_or_else(default_worker_count);

        let concurrent_per_worker = env_usize("RAPPEL_CONCURRENT_PER_WORKER")
            .or_else(|| env_usize("RAPPEL_WORKER_CONCURRENCY"))
            .unwrap_or(DEFAULT_WORKER_CONCURRENCY);

        let user_modules = env::var("RAPPEL_USER_MODULE")
            .ok()
            .map(parse_modules)
            .unwrap_or_default();

        let max_action_lifecycle = env_u64("RAPPEL_MAX_ACTION_LIFECYCLE");

        let poll_interval = Duration::from_millis(
            env_u64("RAPPEL_POLL_INTERVAL_MS").unwrap_or(DEFAULT_POLL_INTERVAL_MS),
        );

        let max_concurrent_instances = env_usize("RAPPEL_MAX_CONCURRENT_INSTANCES")
            .unwrap_or(DEFAULT_MAX_CONCURRENT_INSTANCES);

        let executor_shards =
            env_usize("RAPPEL_EXECUTOR_SHARDS").unwrap_or_else(default_executor_shards);

        let instance_done_batch_size = env_usize("RAPPEL_INSTANCE_DONE_BATCH_SIZE");

        let persistence_interval = Duration::from_millis(
            env_u64("RAPPEL_PERSIST_INTERVAL_MS").unwrap_or(DEFAULT_PERSIST_INTERVAL_MS),
        );

        let lock_ttl =
            Duration::from_millis(env_u64("RAPPEL_LOCK_TTL_MS").unwrap_or(DEFAULT_LOCK_TTL_MS));
        let lock_heartbeat = Duration::from_millis(
            env_u64("RAPPEL_LOCK_HEARTBEAT_MS").unwrap_or(DEFAULT_LOCK_HEARTBEAT_MS),
        );
        let evict_sleep_threshold = Duration::from_millis(
            env_u64("RAPPEL_EVICT_SLEEP_THRESHOLD_MS").unwrap_or(DEFAULT_EVICT_SLEEP_THRESHOLD_MS),
        );
        let expired_lock_reclaimer_interval = Duration::from_millis(
            env_u64("RAPPEL_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS")
                .unwrap_or(DEFAULT_EXPIRED_LOCK_RECLAIMER_INTERVAL_MS)
                .max(1),
        );
        let expired_lock_reclaimer_batch_size =
            env_usize("RAPPEL_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE")
                .unwrap_or(DEFAULT_EXPIRED_LOCK_RECLAIMER_BATCH_SIZE)
                .max(1);

        let scheduler = SchedulerConfig {
            poll_interval: Duration::from_millis(
                env_u64("RAPPEL_SCHEDULER_POLL_INTERVAL_MS")
                    .unwrap_or(DEFAULT_SCHEDULER_POLL_INTERVAL_MS),
            ),
            batch_size: env_i32("RAPPEL_SCHEDULER_BATCH_SIZE")
                .unwrap_or(DEFAULT_SCHEDULER_BATCH_SIZE),
        };

        let webapp = WebappConfig::from_env();

        let profile_interval_ms = env_u64("RAPPEL_RUNNER_PROFILE_INTERVAL_MS")
            .unwrap_or(DEFAULT_PROFILE_INTERVAL_MS)
            .max(1);
        let profile_interval = Duration::from_millis(profile_interval_ms);

        Ok(Self {
            database_url,
            worker_grpc_addr,
            worker_count,
            concurrent_per_worker,
            user_modules,
            max_action_lifecycle,
            poll_interval,
            max_concurrent_instances,
            executor_shards,
            instance_done_batch_size,
            persistence_interval,
            lock_ttl,
            lock_heartbeat,
            evict_sleep_threshold,
            expired_lock_reclaimer_interval,
            expired_lock_reclaimer_batch_size,
            scheduler,
            webapp,
            profile_interval,
        })
    }
}

fn env_u64(var: &str) -> Option<u64> {
    env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn env_usize(var: &str) -> Option<usize> {
    env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
}

fn env_i32(var: &str) -> Option<i32> {
    env::var(var)
        .ok()
        .and_then(|value| value.trim().parse::<i32>().ok())
}

fn default_worker_count() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

fn default_executor_shards() -> usize {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

fn parse_modules(raw: String) -> Vec<String> {
    raw.split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}
