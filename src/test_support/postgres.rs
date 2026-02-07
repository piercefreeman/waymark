//! Shared Postgres fixture bootstrapped from root docker-compose.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::process::Command;
use tokio::sync::OnceCell;

use crate::db;

const POSTGRES_DSN: &str = "postgresql://waymark:waymark@127.0.0.1:5433/waymark";
const READY_TIMEOUT: Duration = Duration::from_secs(45);
const RETRY_DELAY: Duration = Duration::from_millis(500);
const POOL_MAX_CONNECTIONS: u32 = 32;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

static POSTGRES_BOOTSTRAPPED: OnceCell<()> = OnceCell::const_new();

/// Ensure test Postgres is available and migrated, then return a pooled connection.
pub async fn postgres_setup() -> PgPool {
    POSTGRES_BOOTSTRAPPED
        .get_or_init(|| async {
            postgres_setup_impl()
                .await
                .unwrap_or_else(|err| panic!("postgres_setup bootstrap failed: {err:#}"))
        })
        .await;

    connect_pool()
        .await
        .unwrap_or_else(|err| panic!("postgres_setup connect failed: {err:#}"))
}

async fn postgres_setup_impl() -> Result<()> {
    run_compose_up().await?;
    let pool = wait_for_postgres().await?;
    db::run_migrations(&pool)
        .await
        .context("run migrations for test postgres")?;
    pool.close().await;
    Ok(())
}

async fn run_compose_up() -> Result<()> {
    let root = project_root();
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("docker-compose.yml")
        .arg("up")
        .arg("-d")
        .arg("postgres")
        .current_dir(&root)
        .status()
        .await
        .with_context(|| format!("failed to run docker compose in {}", root.display()))?;

    if !status.success() {
        bail!("docker compose up -d postgres exited with status {status}");
    }

    Ok(())
}

async fn wait_for_postgres() -> Result<PgPool> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last_error = None;

    while Instant::now() < deadline {
        match connect_pool().await {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }

    Err(anyhow!(
        "timed out waiting for postgres at {POSTGRES_DSN}; last error: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ))
}

async fn connect_pool() -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
        .connect(POSTGRES_DSN)
        .await?)
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
