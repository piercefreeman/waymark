//! Shared Postgres bootstrap for integration harnesses.

use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::process::Command;
use tokio::sync::OnceCell;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_secret_string::SecretStr;

pub const LOCAL_POSTGRES_DSN: &SecretStr =
    SecretStr::new("postgresql://waymark:waymark@127.0.0.1:5433/waymark");

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const RETRY_DELAY: Duration = Duration::from_millis(500);
const POOL_MAX_CONNECTIONS: u32 = 32;
const POOL_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(15);

const MIGRATIONS_LOCK_HEARTBEAT: NonZeroDuration =
    NonZeroDuration::from_nonzero_secs(NonZeroU64::new(1).unwrap());
const MIGRATIONS_LOCK_TIME_TO_LIVE: NonZeroDuration =
    NonZeroDuration::from_nonzero_secs(NonZeroU64::new(15).unwrap());
const MIGRATIONS_LOCK_FENCING_MARGIN: NonZeroDuration =
    NonZeroDuration::from_nonzero_secs(NonZeroU64::new(1).unwrap());
const MIGRATIONS_LOCK_STATEMENT_TIMEOUT: NonZeroDuration =
    NonZeroDuration::from_nonzero_secs(NonZeroU64::new(3).unwrap());

/// Error returned by [`migrate_main_database`].
#[derive(Debug, thiserror::Error)]
pub enum MigrateMainDatabaseError {
    /// The main database's migrations are not in the order of their
    /// versions.
    #[error("the main database's migrations array: {0}")]
    MigrationsArray(#[source] waymark_migrations_engine::migrations_comparison::ArrayOrderError),

    /// The timing of the migrations lock the helper takes is not valid.
    #[error("migrations lock timing: {0}")]
    LockTiming(#[source] waymark_migrations_engine::migrations_lock_timing::ValidateError),

    /// The main database was not migrated.
    #[error("migrating the main database: {0}")]
    Migrate(
        #[source]
        waymark_postgres_bringup::migrate_and_release_migrations_lock::Error<
            waymark_migrations_engine_assertion::CheckErrorFor<
                waymark_backend_postgres_migrations::Assertions,
            >,
            waymark_migrations_engine::run_migrations::ErrorFor<
                waymark_migrations_engine_backend_sqlx_postgres::Backend<
                    waymark_backend_postgres_migrations::Tables,
                >,
                waymark_backend_postgres_migrations::Assertions,
                waymark_backend_postgres_migrations::Migration,
            >,
        >,
    ),
}

static LOCAL_POSTGRES_BOOTSTRAPPED: OnceCell<()> = OnceCell::const_new();

/// Error returned when connecting a [`PgPool`] fails.
#[derive(Debug, thiserror::Error)]
#[error("connect postgres pool: {0}")]
pub struct ConnectPoolError(#[source] pub sqlx::Error);

/// Error returned when the local Postgres bootstrap fails.
#[derive(Debug, thiserror::Error)]
pub enum EnsureLocalPostgresError {
    /// Applying the database migrations failed.
    #[error("run migrations: {0}")]
    Migrations(#[source] Box<MigrateMainDatabaseError>),

    /// The docker compose command could not be run.
    #[error("run docker compose in {root}: {source}")]
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
    #[error("timed out waiting for postgres{}", waymark_error_fmt::option::suffix(.last_error))]
    WaitTimeout {
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
        pool.close().await;
        migrate_main_database(LOCAL_POSTGRES_DSN)
            .await
            .map_err(EnsureLocalPostgresError::Migrations)?;
        return Ok(());
    }

    run_compose_up().await?;
    let pool = wait_for_postgres(LOCAL_POSTGRES_DSN).await?;
    pool.close().await;
    migrate_main_database(LOCAL_POSTGRES_DSN)
        .await
        .map_err(EnsureLocalPostgresError::Migrations)?;
    Ok(())
}

/// Migrate the main database at `dsn`, the way a tool does: under the
/// exclusive migrations lock, released afterwards.
///
/// Test processes start together, and each migrates: the one that has the
/// lock makes the others' migrations refused. A refused one is tried
/// again until the readiness deadline, by when the one in progress is
/// over and nothing is pending any more.
pub async fn migrate_main_database(dsn: &SecretStr) -> Result<(), Box<MigrateMainDatabaseError>> {
    let migrations_connection = waymark_postgres_config::connection::Config {
        url: dsn.into(),
        statement_timeout: None,
    };

    let migrations_lock_timing = waymark_migrations_engine::MigrationsLockTiming {
        heartbeat: MIGRATIONS_LOCK_HEARTBEAT,
        time_to_live: MIGRATIONS_LOCK_TIME_TO_LIVE,
        fencing_margin: MIGRATIONS_LOCK_FENCING_MARGIN,
    };
    let migrations_lock = waymark_postgres_config::migrations_lock::Config {
        timing: migrations_lock_timing
            .validate()
            .map_err(|error| Box::new(MigrateMainDatabaseError::LockTiming(error)))?,
        statement_timeout: Some(MIGRATIONS_LOCK_STATEMENT_TIMEOUT),
    };
    let assertions = waymark_backend_postgres_migrations::assertions();
    let migrations = waymark_migrations_engine::validate_migrations_array(
        waymark_backend_postgres_migrations::migrations(),
    )
    .map_err(|error| Box::new(MigrateMainDatabaseError::MigrationsArray(error)))?;

    let deadline = Instant::now() + READY_TIMEOUT;

    loop {
        let migrated = waymark_postgres_bringup::migrate_and_release_migrations_lock::<
            waymark_backend_postgres_migrations::Tables,
            _,
            _,
            _,
        >(
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Params {
                migrations_connection: &migrations_connection,
                migrations_lock: &migrations_lock,
                schema: None,
                holder_id: waymark_migrations_engine_backend::migrations_lock::HolderId::from_raw(
                    uuid::Uuid::new_v4(),
                ),
                assertions: &assertions,
                migrations: &migrations,
                transactions: waymark_migrations_engine::MigrationsRunTransactions::WholeRun,
            },
        )
        .await;

        let error = match migrated {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };

        // What refused the lock is looked up after the refusal, so a
        // migration that was over by then shows as a database in use.
        let refused = matches!(
            &error,
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Error::Migrate(
                waymark_postgres_bringup::MigrateUnderExclusiveMigrationsLockError::TakeLock(
                    waymark_migrations_engine::migrations_lock_keeper::TakeExclusiveError::MigrationInProgress
                        | waymark_migrations_engine::migrations_lock_keeper::TakeExclusiveError::DatabaseInUse,
                ),
            )
        );

        if !refused || Instant::now() >= deadline {
            return Err(Box::new(MigrateMainDatabaseError::Migrate(error)));
        }

        tokio::time::sleep(RETRY_DELAY).await;
    }
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

    Err(EnsureLocalPostgresError::WaitTimeout { last_error })
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
