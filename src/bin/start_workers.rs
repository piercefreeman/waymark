use std::sync::Arc;

use anyhow::{Result, anyhow};
use rappel::{
    AppConfig, Store, Dispatcher, DispatcherConfig, PythonWorkerConfig, PythonWorkerPool,
    server_worker::WorkerBridgeServer,
};
use sqlx::postgres::PgPoolOptions;
use tokio::{select, signal};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let app_config = AppConfig::load()?;
    let worker_settings = app_config.worker.clone();
    let worker_count = worker_settings.worker_count.max(1);

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&app_config.database_url)
        .await?;

    let store = Arc::new(Store::new(pool));
    store.init_schema().await?;

    let mut config = PythonWorkerConfig {
        ..PythonWorkerConfig::default()
    };
    if let Some(module) = worker_settings.user_module.clone() {
        config.user_module = module;
    }

    let worker_server = WorkerBridgeServer::start(None).await?;
    let pool =
        Arc::new(PythonWorkerPool::new(config, worker_count, Arc::clone(&worker_server)).await?);

    let dispatcher_config = DispatcherConfig {
        poll_interval: worker_settings.poll_interval,
        batch_size: worker_settings.batch_size,
        max_concurrent: worker_settings.max_concurrent,
    };
    let dispatcher = Dispatcher::start(dispatcher_config, store, Arc::clone(&pool));
    info!(
        worker_count,
        max_concurrent = worker_settings.max_concurrent,
        poll_interval_ms = worker_settings.poll_interval.as_millis(),
        batch_size = worker_settings.batch_size,
        "python worker pool started - waiting for shutdown signal"
    );

    wait_for_shutdown().await?;
    info!("shutdown signal received - stopping workers");
    dispatcher.shutdown().await?;
    let pool = Arc::try_unwrap(pool)
        .map_err(|_| anyhow!("worker pool still referenced during shutdown"))?;
    pool.shutdown().await?;
    worker_server.shutdown().await;
    Ok(())
}

async fn wait_for_shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal as unix_signal};

        let mut terminate = unix_signal(SignalKind::terminate())?;
        select! {
            _ = signal::ctrl_c() => {
                info!("Ctrl+C received");
            }
            _ = terminate.recv() => {
                info!("SIGTERM received");
            }
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        info!("Ctrl+C received");
        Ok(())
    }
}
