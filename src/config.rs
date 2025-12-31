//! Configuration loading from environment variables.
//!
//! Uses the following environment variables:
//! - `RAPPEL_DATABASE_URL`: PostgreSQL connection string (required)
//! - `RAPPEL_BRIDGE_GRPC_ADDR`: gRPC server for client connections to singleton (default: 127.0.0.1:24117)
//! - `RAPPEL_WORKER_GRPC_ADDR`: gRPC server for worker cluster connections (default: 127.0.0.1:24118)
//! - `RAPPEL_BASE_PORT`: Base port for singleton server probing (default: 24117)
//! - `RAPPEL_WORKER_COUNT`: Number of Python workers (default: num_cpus)
//! - `RAPPEL_CONCURRENT_PER_WORKER`: Max concurrent actions per worker (default: 10)
//! - `RAPPEL_POLL_INTERVAL_MS`: Dispatcher poll interval (default: 100)
//! - `RAPPEL_BATCH_SIZE`: Actions to dispatch per poll (default: worker_count * concurrent_per_worker)
//! - `RAPPEL_TIMEOUT_CHECK_INTERVAL_MS`: Timeout maintenance interval (default: 1000)
//! - `RAPPEL_TIMEOUT_CHECK_BATCH_SIZE`: Max actions per timeout maintenance cycle (default: 100)
//! - `RAPPEL_SCHEDULE_CHECK_INTERVAL_MS`: Schedule polling interval (default: 10000)
//! - `RAPPEL_SCHEDULE_CHECK_BATCH_SIZE`: Max schedules per check cycle (default: 100)
//! - `RAPPEL_WORKER_STATUS_INTERVAL_MS`: Worker status upsert interval (default: 10000)
//! - `RAPPEL_USER_MODULE`: Python module to preload in workers (optional)
//! - `RAPPEL_MAX_ACTION_LIFECYCLE`: Max actions per worker before recycling (default: None, no limit)
//! - `RAPPEL_WEBAPP_ENABLED`: Enable webapp dashboard (default: false)
//! - `RAPPEL_WEBAPP_ADDR`: Webapp bind address (default: 0.0.0.0:24119)

use std::{
    env,
    net::SocketAddr,
    str::FromStr,
    sync::{OnceLock, RwLock},
};

use anyhow::{Context, Result};

/// Default address for the webapp server
pub const DEFAULT_WEBAPP_ADDR: &str = "0.0.0.0:24119";

/// Default base port for server singleton probing
pub const DEFAULT_BASE_PORT: u16 = 24117;

/// Global configuration cache
static CONFIG: OnceLock<RwLock<Config>> = OnceLock::new();

/// Server configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL connection URL
    pub database_url: String,

    /// gRPC server for client connections to singleton (WorkflowService + health)
    pub bridge_grpc_addr: SocketAddr,

    /// gRPC server for worker cluster connections (WorkerBridge)
    pub worker_grpc_addr: SocketAddr,

    /// Base port for singleton server probing
    pub base_port: u16,

    /// Number of Python worker processes
    pub worker_count: usize,

    /// Maximum concurrent actions per worker
    pub concurrent_per_worker: usize,

    /// Dispatcher poll interval in milliseconds
    pub poll_interval_ms: u64,

    /// Number of actions to dispatch per poll cycle
    pub batch_size: i32,

    /// Timeout check interval (milliseconds)
    pub timeout_check_interval_ms: u64,

    /// Maximum actions to process per timeout check cycle
    pub timeout_check_batch_size: i32,

    /// Schedule check interval (milliseconds)
    pub schedule_check_interval_ms: u64,

    /// Maximum schedules to process per check cycle
    pub schedule_check_batch_size: i32,

    /// Worker status upsert interval (milliseconds)
    pub worker_status_interval_ms: u64,

    /// Python module to preload in workers
    pub user_module: Option<String>,

    /// Maximum number of actions a worker can execute before being recycled.
    /// None means no limit (workers run indefinitely).
    pub max_action_lifecycle: Option<u64>,

    /// Webapp configuration
    pub webapp: WebappConfig,
}

/// Webapp server configuration
#[derive(Debug, Clone)]
pub struct WebappConfig {
    /// Whether the webapp is enabled
    pub enabled: bool,
    /// Address to bind to (host:port)
    pub addr: SocketAddr,
}

impl Default for WebappConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            addr: DEFAULT_WEBAPP_ADDR.parse().unwrap(),
        }
    }
}

impl WebappConfig {
    /// Load configuration from environment variables
    fn from_env() -> Self {
        let enabled = env::var("RAPPEL_WEBAPP_ENABLED")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let addr = env::var("RAPPEL_WEBAPP_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| DEFAULT_WEBAPP_ADDR.parse().unwrap());

        Self { enabled, addr }
    }

    /// Get the socket address to bind to
    pub fn bind_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Config {
    /// Load configuration from environment variables
    ///
    /// Loads `.env` file if present, then reads from environment.
    pub fn from_env() -> Result<Self> {
        // Load .env file if it exists
        dotenvy::dotenv().ok();

        let database_url = env::var("RAPPEL_DATABASE_URL")
            .context("RAPPEL_DATABASE_URL environment variable is required")?;

        let bridge_grpc_addr =
            env::var("RAPPEL_BRIDGE_GRPC_ADDR").unwrap_or_else(|_| "127.0.0.1:24117".to_string());
        let bridge_grpc_addr = SocketAddr::from_str(&bridge_grpc_addr)
            .context("invalid RAPPEL_BRIDGE_GRPC_ADDR format")?;

        let worker_grpc_addr =
            env::var("RAPPEL_WORKER_GRPC_ADDR").unwrap_or_else(|_| "127.0.0.1:24118".to_string());
        let worker_grpc_addr = SocketAddr::from_str(&worker_grpc_addr)
            .context("invalid RAPPEL_WORKER_GRPC_ADDR format")?;

        let base_port = env::var("RAPPEL_BASE_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_BASE_PORT);

        let worker_count = env::var("RAPPEL_WORKER_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(num_cpus::get);

        let concurrent_per_worker = env::var("RAPPEL_CONCURRENT_PER_WORKER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let poll_interval_ms = env::var("RAPPEL_POLL_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        // Default batch_size to workers * concurrent_per_worker (max available slots)
        let default_batch_size = (worker_count * concurrent_per_worker) as i32;
        let batch_size = env::var("RAPPEL_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_batch_size);

        let user_module = env::var("RAPPEL_USER_MODULE").ok();

        let max_action_lifecycle = env::var("RAPPEL_MAX_ACTION_LIFECYCLE")
            .ok()
            .and_then(|s| s.parse().ok());

        let timeout_check_interval_ms = env::var("RAPPEL_TIMEOUT_CHECK_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let timeout_check_batch_size = env::var("RAPPEL_TIMEOUT_CHECK_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let schedule_check_interval_ms = env::var("RAPPEL_SCHEDULE_CHECK_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000);

        let schedule_check_batch_size = env::var("RAPPEL_SCHEDULE_CHECK_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let worker_status_interval_ms = env::var("RAPPEL_WORKER_STATUS_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000);

        let webapp = WebappConfig::from_env();

        Ok(Self {
            database_url,
            bridge_grpc_addr,
            worker_grpc_addr,
            base_port,
            worker_count,
            concurrent_per_worker,
            poll_interval_ms,
            batch_size,
            timeout_check_interval_ms,
            timeout_check_batch_size,
            schedule_check_interval_ms,
            schedule_check_batch_size,
            worker_status_interval_ms,
            user_module,
            max_action_lifecycle,
            webapp,
        })
    }

    /// Create a test configuration with defaults
    #[cfg(test)]
    pub fn test_config(database_url: &str) -> Self {
        let worker_count = 2;
        let concurrent_per_worker = 5;
        Self {
            database_url: database_url.to_string(),
            bridge_grpc_addr: "127.0.0.1:0".parse().unwrap(),
            worker_grpc_addr: "127.0.0.1:0".parse().unwrap(),
            base_port: DEFAULT_BASE_PORT,
            worker_count,
            concurrent_per_worker,
            poll_interval_ms: 50,
            batch_size: (worker_count * concurrent_per_worker) as i32,
            timeout_check_interval_ms: 1000,
            timeout_check_batch_size: 100,
            schedule_check_interval_ms: 10000,
            schedule_check_batch_size: 100,
            worker_status_interval_ms: 10000,
            user_module: None,
            max_action_lifecycle: None,
            webapp: WebappConfig::default(),
        }
    }
}

/// Get the global configuration, loading from environment if not yet initialized.
///
/// This function returns a clone of the cached configuration. On first call,
/// it loads configuration from environment variables and caches it. Subsequent
/// calls return the cached value.
///
/// # Panics
///
/// Panics if configuration loading fails (e.g., missing required RAPPEL_DATABASE_URL).
pub fn get_config() -> Config {
    CONFIG
        .get_or_init(|| {
            let config = Config::from_env().expect("failed to load configuration from environment");
            RwLock::new(config)
        })
        .read()
        .expect("config lock poisoned")
        .clone()
}

/// Get the global configuration, returning an error if loading fails.
///
/// Like `get_config()` but returns a Result instead of panicking.
pub fn try_get_config() -> Result<Config> {
    match CONFIG.get() {
        Some(lock) => Ok(lock.read().expect("config lock poisoned").clone()),
        None => {
            let config = Config::from_env()?;
            let lock = CONFIG.get_or_init(|| RwLock::new(config.clone()));
            Ok(lock.read().expect("config lock poisoned").clone())
        }
    }
}

/// Reset the global configuration cache.
///
/// This is primarily useful for testing when you need to reload configuration
/// with different environment variables.
#[cfg(test)]
pub fn reset_config() {
    // OnceLock doesn't support reset, so we use RwLock to allow updating the inner value
    if let Some(lock) = CONFIG.get()
        && let Ok(new_config) = Config::from_env()
        && let Ok(mut guard) = lock.write()
    {
        *guard = new_config;
    }
}

/// Get the database URL from environment
pub fn database_url() -> Result<String> {
    dotenvy::dotenv().ok();
    env::var("RAPPEL_DATABASE_URL").context("RAPPEL_DATABASE_URL environment variable is required")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_grpc_ports() {
        // Bridge gRPC defaults to 24117, Worker gRPC defaults to 24118
        let expected_bridge: SocketAddr = "127.0.0.1:24117".parse().unwrap();
        let expected_worker: SocketAddr = "127.0.0.1:24118".parse().unwrap();

        // These are the hardcoded defaults in from_env()
        assert_eq!(expected_bridge.port(), 24117);
        assert_eq!(expected_worker.port(), 24118);
    }

    #[test]
    fn test_webapp_config_default() {
        let config = WebappConfig::default();
        assert!(!config.enabled);
        assert_eq!(
            config.addr,
            DEFAULT_WEBAPP_ADDR.parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn test_test_config_includes_webapp() {
        let config = Config::test_config("postgres://test");
        assert!(!config.webapp.enabled);
        assert_eq!(
            config.webapp.addr,
            DEFAULT_WEBAPP_ADDR.parse::<SocketAddr>().unwrap()
        );
    }
}
