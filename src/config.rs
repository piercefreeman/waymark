//! Server configuration.

use std::net::SocketAddr;

/// Server configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// PostgreSQL connection URL
    pub database_url: String,

    /// HTTP server bind address
    pub http_addr: SocketAddr,

    /// gRPC server bind address for action workers
    pub action_grpc_addr: SocketAddr,

    /// gRPC server bind address for instance workers
    pub instance_grpc_addr: SocketAddr,

    /// Number of action workers to spawn
    pub action_worker_count: usize,

    /// Number of instance workers to spawn
    pub instance_worker_count: usize,

    /// Max concurrent actions per action worker
    pub max_concurrent_per_action_worker: usize,

    /// Max concurrent instances per instance worker
    pub max_concurrent_per_instance_worker: usize,

    /// Python modules to load (contain workflows and actions)
    pub user_modules: Vec<String>,
}

impl Config {
    /// Load configuration from environment variables.
    pub fn from_env() -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL must be set"))?;

        let http_addr: SocketAddr = std::env::var("RAPPEL_HTTP_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:24117".to_string())
            .parse()?;

        let action_grpc_addr: SocketAddr = std::env::var("RAPPEL_ACTION_GRPC_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:24118".to_string())
            .parse()?;

        let instance_grpc_addr: SocketAddr = std::env::var("RAPPEL_INSTANCE_GRPC_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:24119".to_string())
            .parse()?;

        let action_worker_count: usize = std::env::var("RAPPEL_ACTION_WORKER_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(num_cpus::get);

        let instance_worker_count: usize = std::env::var("RAPPEL_INSTANCE_WORKER_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4);

        let max_concurrent_per_action_worker: usize =
            std::env::var("RAPPEL_MAX_CONCURRENT_PER_ACTION_WORKER")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);

        let max_concurrent_per_instance_worker: usize =
            std::env::var("RAPPEL_MAX_CONCURRENT_PER_INSTANCE_WORKER")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5);

        let user_modules: Vec<String> = std::env::var("RAPPEL_USER_MODULES")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.trim().to_string())
            .collect();

        Ok(Config {
            database_url,
            http_addr,
            action_grpc_addr,
            instance_grpc_addr,
            action_worker_count,
            instance_worker_count,
            max_concurrent_per_action_worker,
            max_concurrent_per_instance_worker,
            user_modules,
        })
    }
}
