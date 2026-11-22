use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use color_eyre::eyre::{WrapErr as _, bail};
use sqlx::PgPool;
use tokio::process::Command;
use waymark_nonzero_duration::NonZeroDuration;
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

/// Connect the main pool, migrated, waiting up to `timeout` for the
/// database to accept connections; every connection, the migrations'
/// included, runs its statements under `statement_timeout`.
///
/// The database is migrated through a connection of its own, under the
/// exclusive migrations lock, which is released afterwards: the harness
/// takes no migrations lock for its own statements.
pub async fn connect(
    dsn: &SecretStr,
    statement_timeout: NonZeroDuration,
    timeout: Duration,
) -> Result<PgPool, color_eyre::eyre::Report> {
    let migrations_connection = waymark_postgres_config::connection::Config {
        url: dsn.into(),
        statement_timeout: Some(statement_timeout),
    };
    let migrations_lock = crate::common::migrations_lock_config()
        .wrap_err("the timing of the harness's migrations lock")?;
    let assertions = waymark_backend_postgres_migrations::assertions();
    let migrations = waymark_migrations_engine::validate_migrations_array(
        waymark_backend_postgres_migrations::migrations(),
    )
    .wrap_err("the main database's migrations array")?;

    crate::common::wait_for_database("the main database", timeout, || async {
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
                    waymark_ids::NodeId::new_uuid_v4().into(),
                ),
                assertions: &assertions,
                migrations: &migrations,
                transactions: waymark_migrations_engine::MigrationsRunTransactions::WholeRun,
            },
        )
        .await;

        let Err(error) = migrated else {
            return Ok(());
        };

        // Only a connect another attempt may get past is waited out; a bad
        // URL, a permanent connect failure, a refused migrations lock, a
        // migration failure or a failed close is not the database coming
        // up, and no migration runs twice.
        let retry = matches!(
            &error,
            waymark_postgres_bringup::migrate_and_release_migrations_lock::Error::ConnectForMigrations(
                waymark_postgres_bringup::ConnectionError::Connect(connect_error),
            ) if !is_permanent(connect_error)
        );

        if retry {
            Err(crate::common::WaitForDatabaseAttemptError::Retry(error))
        } else {
            Err(crate::common::WaitForDatabaseAttemptError::Stop(error))
        }
    })
    .await
    .wrap_err("migrate the database before soak")?;

    let pool = waymark_postgres_bringup::pool(waymark_postgres_bringup::Params {
        config: &waymark_postgres_config::pool::Config {
            url: dsn.into(),
            max_connections: NonZeroU32::new(16).expect("non-zero"),
            statement_timeout: Some(statement_timeout),
            acquire_timeout: NonZeroDuration::from_secs(5).expect("non-zero"),
        },
        transactions: waymark_postgres_bringup::Transactions::Unrestricted,
        schema: None,
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
