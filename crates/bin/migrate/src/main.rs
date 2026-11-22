//! Waymark Migrate - inspect and run the migrations of one Waymark database.
//!
//! Usage:
//!   `waymark-migrate <main-database|observability-store> status`
//!   `waymark-migrate <main-database|observability-store> check`
//!   `waymark-migrate <main-database|observability-store> run --transactions <whole-run|unmanaged>`
//!
//! `status` and `check` write nothing and take no migrations lock; `run`
//! applies the pending migrations under the exclusive migrations lock,
//! which it refuses to take while a node or a bridge runs on the database
//! or another migration is in progress. There are no rollbacks.
//!
//! The logs go to the standard output, as every Waymark binary's do, and
//! one JSON object is written there last, on a line of its own, per
//! invocation.
//!
//! Exit codes:
//! - 0: `status` reported; `check` found the database migrated exactly to this build's migrations; `run` applied the pending migrations, or none was pending
//! - 1: the command failed
//! - 2: the command line is not valid
//! - 3: `check` found migrations pending, or a database that was never migrated
//! - 4: `check` found a migration in progress; `run` was refused the exclusive migrations lock
//! - 5: a migration was started and never finished
//! - 6: the migrations ledger conflicts with this build's migrations
//! - 7: `check` and `run` found a database that does not meet an assertion of the migrations engine: one an earlier Waymark migrated, whose migrations ledger was not moved yet
//!
//! Configuration via environment variables, the ones a node migrates
//! with. Only the unified variables and the variables of the database
//! acted on are read:
//! - WAYMARK_DATABASE_URL: PostgreSQL connection string for every database that has no URL of its own (required unless the database acted on has one)
//! - WAYMARK_MIGRATIONS_DATABASE_URL: DSN the migrations run through for every database that has no migrations URL and no URL of its own (default: WAYMARK_DATABASE_URL)
//! - WAYMARK_MAIN_DATABASE_URL: DSN for the main database (default: WAYMARK_DATABASE_URL)
//! - WAYMARK_MAIN_MIGRATIONS_DATABASE_URL: DSN the main database's migrations run through (default: WAYMARK_MAIN_DATABASE_URL when set, else WAYMARK_MIGRATIONS_DATABASE_URL)
//! - WAYMARK_MAIN_POSTGRES_MIGRATIONS_STATEMENT_TIMEOUT_MS: Statement timeout on the main database's migrations connection, `0` for none (default: 600000)
//! - WAYMARK_MAIN_POSTGRES_MIGRATIONS_LOCK_TTL_MS / WAYMARK_MAIN_POSTGRES_MIGRATIONS_LOCK_HEARTBEAT_MS / WAYMARK_MAIN_POSTGRES_MIGRATIONS_LOCK_FENCING_MARGIN_MS / WAYMARK_MAIN_POSTGRES_MIGRATIONS_LOCK_STATEMENT_TIMEOUT_MS: The main database's migrations lock (defaults: 15000 / 5000 / 1000 / 3000)
//! - WAYMARK_OBSERVABILITY_DATABASE_URL: DSN for the observability store (default: WAYMARK_DATABASE_URL)
//! - WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL: DSN the observability migrations run through (default: WAYMARK_OBSERVABILITY_DATABASE_URL when set, else WAYMARK_MIGRATIONS_DATABASE_URL)
//! - WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_STATEMENT_TIMEOUT_MS: Statement timeout on the observability migrations connection, `0` for none (default: 600000)
//! - WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_TTL_MS / WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_HEARTBEAT_MS / WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_FENCING_MARGIN_MS / WAYMARK_OBSERVABILITY_POSTGRES_MIGRATIONS_LOCK_STATEMENT_TIMEOUT_MS: The observability store's migrations lock (defaults: 15000 / 5000 / 1000 / 3000)
//! - RUST_LOG: Log filter (default: info)

mod cli;
mod report;
mod run;
mod status;

use waymark_migrations_engine::ValidMigrationsArray;
use waymark_postgres_config::{connection, migrations_lock};

use self::cli::{Cli, Command, Database};
use self::report::{Body, Exit, Report};

/// The schema the observability store's tables are in. Hardcoded for now —
/// mirrors `waymark-observability-bringup`.
const OBSERVABILITY_SCHEMA: &str = "observability";

/// The database an invocation acts on.
#[derive(Debug)]
struct Target<'a, Assertion, Migrations> {
    /// The config of the connection to the database: the one its
    /// migrations run through.
    connection: &'a connection::Config,

    /// The config of the database's migrations lock.
    migrations_lock: &'a migrations_lock::Config,

    /// The schema the database's tables are in; unscoped when `None`.
    schema: Option<&'a str>,

    /// The assertions the database has to meet.
    assertions: &'a Assertion,

    /// The migrations of this build for the database.
    migrations: &'a ValidMigrationsArray<Migrations>,
}

/// Error of the common initialization of the process.
#[derive(Debug, thiserror::Error)]
#[error("initializing the process: {0}")]
struct InitError(#[source] waymark_fn_main_common::InitError);

/// Error of reading the config of the database to act on.
#[derive(Debug, thiserror::Error)]
enum ConfigError {
    /// The main database's config could not be read.
    #[error("main database: {0}")]
    MainDatabase(#[source] waymark_backend_postgres_config::migrate_only::FromEnvError),

    /// The observability store's config could not be read.
    #[error("observability store: {0}")]
    ObservabilityStore(#[source] waymark_observability_store_config::migrate_only::FromEnvError),
}

/// Error of a command.
#[derive(Debug, thiserror::Error)]
enum Error<AssertionCheckError, RunError> {
    /// The config could not be read.
    #[error("config: {0}")]
    Config(#[source] ConfigError),

    /// The migrations of this build for the database are not in the order
    /// of their versions.
    #[error("migrations array: {0}")]
    MigrationsArray(#[source] waymark_migrations_engine::migrations_comparison::ArrayOrderError),

    /// The status of the migrations could not be told.
    #[error("migrations status: {0}")]
    Status(#[source] status::Error<AssertionCheckError>),

    /// The migrations run failed.
    #[error("migrations run: {0}")]
    Run(#[source] RunError),
}

/// What a command that did not fail came to.
#[derive(Debug)]
enum Executed {
    /// The `status` command read the status.
    Status(report::Status),

    /// The `check` command read the status.
    Check(report::Status),

    /// The `run` command ran.
    Run(report::Run),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = <Cli as clap::Parser>::parse();

    let (body, exit) = match waymark_fn_main_common::init() {
        Ok(()) => match cli.database {
            Database::MainDatabase => conclude(main_database(&cli.command).await),
            Database::ObservabilityStore => conclude(observability_store(&cli.command).await),
        },
        Err(error) => {
            let body = Body::Error {
                error: InitError(error).to_string(),
            };

            (body, Exit::Error)
        }
    };

    let report = Report {
        database: cli.database.label(),
        command: cli.command.label(),
        body,
    };

    report::write(&report, exit)
}

/// The report's body and the exit code of a command.
fn conclude<AssertionCheckError, RunError>(
    executed: Result<Executed, Error<AssertionCheckError, RunError>>,
) -> (Body, Exit)
where
    AssertionCheckError: core::fmt::Display,
    RunError: core::fmt::Display,
{
    match executed {
        Ok(Executed::Status(status)) => (Body::Status(status), Exit::Done),
        Ok(Executed::Check(status)) => {
            let exit = report::check_exit(&status);

            (Body::Status(status), exit)
        }
        Ok(Executed::Run(run)) => {
            let exit = report::run_exit(&run);

            (Body::Run(run), exit)
        }
        Err(error) => {
            tracing::error!(%error, "the command failed");

            let body = Body::Error {
                error: error.to_string(),
            };

            (body, Exit::Error)
        }
    }
}

/// Run `command` on the main database.
async fn main_database(
    command: &Command,
) -> Result<
    Executed,
    Error<
        waymark_migrations_engine_assertion::CheckErrorFor<
            waymark_backend_postgres_migrations::Assertions,
        >,
        run::Error<
            waymark_backend_postgres_migrations::Tables,
            waymark_backend_postgres_migrations::Assertions,
            waymark_backend_postgres_migrations::Migration,
        >,
    >,
> {
    let config = waymark_backend_postgres_config::MigrateOnly::from_env()
        .map_err(|error| Error::Config(ConfigError::MainDatabase(error)))?;
    let assertions = waymark_backend_postgres_migrations::assertions();
    let migrations = waymark_migrations_engine::validate_migrations_array(
        waymark_backend_postgres_migrations::migrations(),
    )
    .map_err(Error::MigrationsArray)?;

    execute::<waymark_backend_postgres_migrations::Tables, _, _, _>(
        Target {
            connection: &config.migrations,
            migrations_lock: &config.migrations_lock,
            schema: None,
            assertions: &assertions,
            migrations: &migrations,
        },
        command,
    )
    .await
}

/// Run `command` on the observability store.
async fn observability_store(
    command: &Command,
) -> Result<
    Executed,
    Error<
        waymark_migrations_engine_assertion::CheckErrorFor<
            waymark_observability_store_postgres_migrations::Assertions,
        >,
        run::Error<
            waymark_observability_store_postgres_migrations::Tables,
            waymark_observability_store_postgres_migrations::Assertions,
            waymark_observability_store_postgres_migrations::Migration,
        >,
    >,
> {
    let config = waymark_observability_store_config::MigrateOnly::from_env()
        .map_err(|error| Error::Config(ConfigError::ObservabilityStore(error)))?;
    let waymark_observability_store_config::MigrateOnly::Postgres(config) = config;
    let assertions = waymark_observability_store_postgres_migrations::assertions();
    let migrations = waymark_migrations_engine::validate_migrations_array(
        waymark_observability_store_postgres_migrations::migrations(),
    )
    .map_err(Error::MigrationsArray)?;

    execute::<waymark_observability_store_postgres_migrations::Tables, _, _, _>(
        Target {
            connection: &config.migrations,
            migrations_lock: &config.migrations_lock,
            schema: Some(OBSERVABILITY_SCHEMA),
            assertions: &assertions,
            migrations: &migrations,
        },
        command,
    )
    .await
}

/// Run `command` on the database of `target`.
async fn execute<Tables, Assertion, Migrations, Migration>(
    target: Target<'_, Assertion, Migrations>,
    command: &Command,
) -> Result<
    Executed,
    Error<
        waymark_migrations_engine_assertion::CheckErrorFor<Assertion>,
        run::Error<Tables, Assertion, Migration>,
    >,
>
where
    Tables: waymark_migrations_engine_backend_sqlx_postgres::Tables,
    Assertion: waymark_migrations_engine_assertion::Assertion<Connection = sqlx::PgConnection>,
    Migrations: AsRef<[Migration]>,
    Migration: waymark_migrations_engine_migration::Migration<Connection = sqlx::PgConnection>,
{
    match command {
        Command::Status => status::status::<Tables, _, _, _>(&target)
            .await
            .map(Executed::Status)
            .map_err(Error::Status),
        Command::Check => status::status::<Tables, _, _, _>(&target)
            .await
            .map(Executed::Check)
            .map_err(Error::Status),
        Command::Run { transactions } => run::run::<Tables, _, _, _>(&target, transactions)
            .await
            .map(Executed::Run)
            .map_err(Error::Run),
    }
}
