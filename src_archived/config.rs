use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use dotenvy::from_path_iter;
use tracing::info;

const DOTENV_FILENAME: &str = ".env";
const HTTP_ADDR_ENV: &str = "CARABINER_HTTP_ADDR";
const GRPC_ADDR_ENV: &str = "CARABINER_GRPC_ADDR";
const WORKER_COUNT_ENV: &str = "CARABINER_WORKER_COUNT";
const WORKER_MAX_CONCURRENT_ENV: &str = "CARABINER_MAX_CONCURRENT";
const WORKER_USER_MODULE_ENV: &str = "CARABINER_USER_MODULE";
const WORKER_POLL_INTERVAL_ENV: &str = "CARABINER_POLL_INTERVAL_MS";
const WORKER_BATCH_SIZE_ENV: &str = "CARABINER_BATCH_SIZE";

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub http_addr: Option<SocketAddr>,
    pub grpc_addr: Option<SocketAddr>,
    pub worker: WorkerRuntimeConfig,
}

#[derive(Debug, Clone)]
pub struct WorkerRuntimeConfig {
    pub worker_count: usize,
    pub max_concurrent: usize,
    pub user_module: Option<String>,
    pub poll_interval: Duration,
    pub batch_size: i64,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        if let Some(path) = hydrate_env_from_files()? {
            info!(env_file = %path.display(), "loaded configuration from env file");
        }
        let database_url = env::var("DATABASE_URL")
            .context("DATABASE_URL missing; set it in the environment or .env file")?;
        let http_addr = parse_socket_addr_from_env(HTTP_ADDR_ENV)?;
        let grpc_addr = parse_socket_addr_from_env(GRPC_ADDR_ENV)?;
        let worker = WorkerRuntimeConfig::load()?;
        Ok(Self {
            database_url,
            http_addr,
            grpc_addr,
            worker,
        })
    }
}

fn hydrate_env_from_files() -> Result<Option<PathBuf>> {
    let mut current = env::current_dir().context("failed to resolve current directory")?;
    loop {
        let candidate = current.join(DOTENV_FILENAME);
        if candidate.is_file() {
            load_env_file(&candidate)?;
            return Ok(Some(candidate));
        }
        if !current.pop() {
            break;
        }
    }
    Ok(None)
}

fn load_env_file(path: &Path) -> Result<()> {
    for entry in from_path_iter(path)? {
        let (key, value) = entry?;
        if env::var_os(&key).is_some() {
            continue;
        }
        // SAFETY: set_var is unsafe because callers must avoid concurrent mutations.
        // Configuration loading happens during startup before any worker threads spawn.
        unsafe {
            env::set_var(&key, &value);
        }
    }
    Ok(())
}

fn parse_socket_addr_from_env(key: &str) -> Result<Option<SocketAddr>> {
    let value = match env::var(key) {
        Ok(raw) => raw,
        Err(_) => return Ok(None),
    };
    let addr = value
        .parse::<SocketAddr>()
        .with_context(|| format!("{key} must be a valid socket address, e.g. 0.0.0.0:1234"))?;
    Ok(Some(addr))
}

impl WorkerRuntimeConfig {
    fn load() -> Result<Self> {
        let worker_count = match env::var(WORKER_COUNT_ENV) {
            Ok(raw) => {
                let parsed = raw
                    .parse::<usize>()
                    .with_context(|| format!("{WORKER_COUNT_ENV} must be a positive integer"))?;
                if parsed == 0 {
                    return Err(anyhow!(
                        "{WORKER_COUNT_ENV} must be greater than zero when provided"
                    ));
                }
                parsed
            }
            Err(_) => num_cpus::get().max(1),
        };
        let max_concurrent = match env::var(WORKER_MAX_CONCURRENT_ENV) {
            Ok(raw) => {
                let parsed = raw.parse::<usize>().with_context(|| {
                    format!("{WORKER_MAX_CONCURRENT_ENV} must be a positive integer")
                })?;
                if parsed == 0 {
                    return Err(anyhow!(
                        "{WORKER_MAX_CONCURRENT_ENV} must be greater than zero when provided"
                    ));
                }
                parsed
            }
            Err(_) => 32,
        };
        let poll_interval_ms = match env::var(WORKER_POLL_INTERVAL_ENV) {
            Ok(raw) => {
                let parsed = raw.parse::<u64>().with_context(|| {
                    format!("{WORKER_POLL_INTERVAL_ENV} must be a positive integer in milliseconds")
                })?;
                if parsed == 0 {
                    return Err(anyhow!(
                        "{WORKER_POLL_INTERVAL_ENV} must be greater than zero when provided"
                    ));
                }
                parsed
            }
            Err(_) => 100,
        };
        let batch_size = match env::var(WORKER_BATCH_SIZE_ENV) {
            Ok(raw) => {
                let parsed = raw.parse::<i64>().with_context(|| {
                    format!("{WORKER_BATCH_SIZE_ENV} must be a positive integer")
                })?;
                if parsed <= 0 {
                    return Err(anyhow!(
                        "{WORKER_BATCH_SIZE_ENV} must be greater than zero when provided"
                    ));
                }
                parsed
            }
            Err(_) => 100,
        };
        Ok(Self {
            worker_count,
            max_concurrent,
            user_module: env::var(WORKER_USER_MODULE_ENV).ok(),
            poll_interval: Duration::from_millis(poll_interval_ms),
            batch_size,
        })
    }
}
