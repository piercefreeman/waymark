use std::path::PathBuf;
use std::time::Duration;

use color_eyre::eyre::{WrapErr as _, bail};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::process::Command;
use waymark_secret_string::SecretStr;

pub async fn boot_postgres() -> Result<(), color_eyre::eyre::Report> {
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
        .wrap_err_with(|| format!("run docker compose from {}", project_root.display()))?;

    if !status.success() {
        bail!("docker compose up -d postgres failed with status {status}");
    }

    Ok(())
}

pub async fn connect(
    dsn: &SecretStr,
    timeout: Duration,
) -> Result<PgPool, color_eyre::eyre::Report> {
    let what = format!("Postgres at {}", dsn.expose_secret());
    let pool = crate::common::wait_for_database(&what, timeout, || async {
        PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(dsn.expose_secret())
            .await
            .map_err(crate::common::WaitForDatabaseAttemptError::<_, sqlx::Error>::Retry)
    })
    .await
    .wrap_err("connect to the database")?;
    Ok(pool)
}
