//! The command line of the tool.

/// Inspect and run the migrations of one Waymark database.
#[derive(Debug, clap::Parser)]
#[command(name = "waymark-migrate")]
pub struct Cli {
    /// The database to act on.
    #[arg(value_enum)]
    pub database: Database,

    /// What to do with its migrations.
    #[command(subcommand)]
    pub command: Command,
}

/// The databases the tool acts on, one per invocation.
// `Clone` is what `clap::ValueEnum` asks of an argument's value.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Database {
    /// The main database.
    MainDatabase,

    /// The observability store.
    ObservabilityStore,
}

impl Database {
    /// The database as the command line and the report spell it.
    pub fn label(&self) -> &'static str {
        match self {
            Self::MainDatabase => "main-database",
            Self::ObservabilityStore => "observability-store",
        }
    }
}

/// What the tool does with the migrations of the database.
#[derive(Debug, clap::Subcommand)]
pub enum Command {
    /// Report how the database stands against this build's migrations.
    /// Nothing is written.
    Status,

    /// Report the same, and exit with a code that tells whether the
    /// database is migrated exactly to this build's migrations. Nothing
    /// is written.
    Check,

    /// Apply the pending migrations under the exclusive migrations lock.
    Run {
        /// What the run does about transactions: `whole-run` applies every
        /// pending migration in one transaction and refuses up front when
        /// one of them cannot run in a transaction; `unmanaged` runs each
        /// migration with no transaction around it.
        #[arg(long, value_enum)]
        transactions: Transactions,
    },
}

impl Command {
    /// The command as the command line and the report spell it.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Check => "check",
            Self::Run { .. } => "run",
        }
    }
}

/// What a run does about transactions.
// `Clone` is what `clap::ValueEnum` asks of an argument's value.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Transactions {
    /// One transaction around the whole run.
    WholeRun,

    /// No transaction around any migration.
    Unmanaged,
}
