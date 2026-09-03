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
    let pool = crate::common::wait_for_database("the main database", timeout, || async {
        PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(Duration::from_secs(5))
            .connect(dsn.expose_secret())
            .await
            .map_err(|error| {
                if is_permanent(&error) {
                    crate::common::WaitForDatabaseAttemptError::Stop(error)
                } else {
                    crate::common::WaitForDatabaseAttemptError::Retry(error)
                }
            })
    })
    .await
    .wrap_err("connect to the database")?;
    Ok(pool)
}

/// Whether a failed connect is one another attempt meets again: a bad
/// URL or TLS setup, a wrong password, a role or client the server
/// refuses, a database that does not exist. The server not being up yet
/// is not.
fn is_permanent(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Configuration(_) | sqlx::Error::Tls(_) => true,
        sqlx::Error::Database(error) => {
            let code = error.code();
            matches!(code.as_deref(), Some("28P01" | "28000" | "3D000"))
        }
        _ => false,
    }
}
