//! Shared Postgres bootstrap for integration harnesses.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::process::Command;
use tokio::sync::OnceCell;

pub const LOCAL_POSTGRES_DSN: &str = "postgresql://waymark:waymark@127.0.0.1:5433/waymark";

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const RETRY_DELAY: Duration = Duration::from_millis(500);
const POOL_MAX_CONNECTIONS: u32 = 32;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

static LOCAL_POSTGRES_BOOTSTRAPPED: OnceCell<()> = OnceCell::const_new();

/// Ensure the default local Postgres is available and migrated.
///
/// This helper is intended for local integration workflows where the default
/// DSN maps to the repository docker-compose service.
pub async fn ensure_local_postgres() -> Result<()> {
    LOCAL_POSTGRES_BOOTSTRAPPED
        .get_or_try_init(|| async { ensure_local_postgres_impl().await })
        .await?;
    Ok(())
}

/// Connect a PgPool using integration defaults.
pub async fn connect_pool(dsn: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
        .connect(dsn)
        .await?)
}

async fn ensure_local_postgres_impl() -> Result<()> {
    if let Ok(pool) = connect_pool(LOCAL_POSTGRES_DSN).await {
        waymark_backend_postgres_migrations::run(&pool)
            .await
            .context("run migrations for existing local postgres")?;
        pool.close().await;
        return Ok(());
    }

    run_compose_up().await?;
    let pool = wait_for_postgres(LOCAL_POSTGRES_DSN).await?;
    waymark_backend_postgres_migrations::run(&pool)
        .await
        .context("run migrations for local postgres")?;
    pool.close().await;
    Ok(())
}

async fn run_compose_up() -> Result<()> {
    let root = project_root();
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("../../docker-compose.yml")
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

async fn wait_for_postgres(dsn: &str) -> Result<PgPool> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last_error = None;

    while Instant::now() < deadline {
        match connect_pool(dsn).await {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_error = Some(err);
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }

    Err(anyhow!(
        "timed out waiting for postgres at {dsn}; last error: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ))
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
