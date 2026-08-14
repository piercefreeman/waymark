//! Shared Postgres bootstrap for integration harnesses.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::process::Command;
use tokio::sync::OnceCell;
use waymark_secret_string::SecretStr;

pub const LOCAL_POSTGRES_DSN: &SecretStr =
    SecretStr::new("postgresql://waymark:waymark@127.0.0.1:5433/waymark");

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const RETRY_DELAY: Duration = Duration::from_millis(500);
const POOL_MAX_CONNECTIONS: u32 = 32;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

static LOCAL_POSTGRES_BOOTSTRAPPED: OnceCell<()> = OnceCell::const_new();

/// Error returned when connecting a [`PgPool`] fails.
#[derive(Debug, thiserror::Error)]
#[error("connect postgres pool")]
pub struct ConnectPoolError(#[source] pub sqlx::Error);

/// Error returned when the local Postgres bootstrap fails.
#[derive(Debug, thiserror::Error)]
pub enum EnsureLocalPostgresError {
    /// Applying the database migrations failed.
    #[error("run migrations: {0}")]
    Migrations(#[source] sqlx::migrate::MigrateError),

    /// The docker compose command could not be run.
    #[error("run docker compose in {root}")]
    ComposeRun {
        /// The directory docker compose ran in.
        root: PathBuf,

        /// The underlying spawn/wait error.
        #[source]
        source: std::io::Error,
    },

    /// The docker compose command reported a failure.
    #[error("docker compose up -d postgres exited with status {0}")]
    ComposeStatus(std::process::ExitStatus),

    /// Postgres did not accept connections before the deadline.
    #[error("timed out waiting for postgres at {dsn}")]
    WaitTimeout {
        /// The DSN that was being connected to.
        dsn: String,

        /// The connection error from the last attempt.
        #[source]
        last_error: Option<ConnectPoolError>,
    },
}

/// Ensure the default local Postgres is available and migrated.
///
/// This helper is intended for local integration workflows where the default
/// DSN maps to the repository docker-compose service.
pub async fn ensure_local_postgres() -> Result<(), EnsureLocalPostgresError> {
    LOCAL_POSTGRES_BOOTSTRAPPED
        .get_or_try_init(|| async { ensure_local_postgres_impl().await })
        .await?;
    Ok(())
}

/// Connect a PgPool using integration defaults.
pub async fn connect_pool(dsn: &SecretStr) -> Result<PgPool, ConnectPoolError> {
    PgPoolOptions::new()
        .max_connections(POOL_MAX_CONNECTIONS)
        .acquire_timeout(POOL_ACQUIRE_TIMEOUT)
        .connect(dsn.expose_secret())
        .await
        .map_err(ConnectPoolError)
}

async fn ensure_local_postgres_impl() -> Result<(), EnsureLocalPostgresError> {
    if let Ok(pool) = connect_pool(LOCAL_POSTGRES_DSN).await {
        waymark_backend_postgres_migrations::run(&pool)
            .await
            .map_err(EnsureLocalPostgresError::Migrations)?;
        pool.close().await;
        return Ok(());
    }

    run_compose_up().await?;
    let pool = wait_for_postgres(LOCAL_POSTGRES_DSN).await?;
    waymark_backend_postgres_migrations::run(&pool)
        .await
        .map_err(EnsureLocalPostgresError::Migrations)?;
    pool.close().await;
    Ok(())
}

async fn run_compose_up() -> Result<(), EnsureLocalPostgresError> {
    let root = project_root();
    let status = Command::new("docker")
        .arg("compose")
        .arg("-f")
        .arg("../../../docker-compose.yml")
        .arg("up")
        .arg("-d")
        .arg("postgres")
        .current_dir(&root)
        .status()
        .await
        .map_err(|source| EnsureLocalPostgresError::ComposeRun { root, source })?;

    if !status.success() {
        return Err(EnsureLocalPostgresError::ComposeStatus(status));
    }

    Ok(())
}

async fn wait_for_postgres(dsn: &SecretStr) -> Result<PgPool, EnsureLocalPostgresError> {
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

    Err(EnsureLocalPostgresError::WaitTimeout {
        dsn: dsn.expose_secret().to_string(),
        last_error,
    })
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
