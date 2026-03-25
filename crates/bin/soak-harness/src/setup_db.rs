use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::process::Command;
use waymark_secret_string::SecretStr;

const DB_RETRY_DELAY: Duration = Duration::from_millis(500);

pub async fn boot_postgres() -> Result<()> {
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("../../../docker-compose.yml")
        .arg("up")
        .arg("-d")
        .arg("postgres")
        .current_dir(&project_root)
        .status()
        .await
        .with_context(|| format!("run docker compose from {}", project_root.display()))?;

    if !status.success() {
        bail!("docker compose up -d postgres failed with status {status}");
    }

    Ok(())
}

pub async fn wait_for_database(dsn: &SecretStr, timeout: Duration) -> Result<PgPool> {
    let deadline = Instant::now() + timeout;
    let mut last_error: Option<String> = None;

    while Instant::now() < deadline {
        match PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(dsn.expose_secret())
            .await
        {
            Ok(pool) => return Ok(pool),
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(DB_RETRY_DELAY).await;
            }
        }
    }

    Err(anyhow!(
        "timed out waiting for Postgres at {}; last error: {}",
        dsn.expose_secret(),
        last_error.unwrap_or_else(|| "unknown".to_string())
    ))
}
