use std::sync::Arc;

use anyhow::Result;
use carabiner::{PythonWorkerConfig, PythonWorkerPool, server_worker::WorkerBridgeServer};
use clap::Parser;
use tokio::signal;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "start_workers",
    about = "Launch Python worker pool for carabiner"
)]
struct Args {
    /// Number of worker processes to spawn (defaults to number of CPUs).
    #[arg(long)]
    workers: Option<usize>,
    /// Python module containing user-defined actions to preload.
    #[arg(long)]
    user_module: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let worker_count = args.workers.unwrap_or_else(|| num_cpus::get().max(1));

    let mut config = PythonWorkerConfig::default();
    if let Some(module) = args.user_module {
        config.user_module = module;
    }

    let worker_server = WorkerBridgeServer::start(None).await?;
    let pool = PythonWorkerPool::new(config, worker_count, Arc::clone(&worker_server)).await?;
    info!(
        worker_count,
        "python worker pool started - waiting for shutdown signal"
    );

    signal::ctrl_c().await?;
    info!("shutdown signal received - stopping workers");
    pool.shutdown().await?;
    worker_server.shutdown().await;
    Ok(())
}
