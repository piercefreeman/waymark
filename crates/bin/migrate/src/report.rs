//! What the tool reports: one JSON object, the last line on the standard
//! output, and the exit code.

use std::io::Write as _;

/// The one JSON object an invocation writes to the standard output.
#[derive(Debug, serde::Serialize)]
pub struct Report {
    /// The database acted on, as the command line spells it.
    pub database: &'static str,

    /// The command, as the command line spells it.
    pub command: &'static str,

    /// What came out of it.
    #[serde(flatten)]
    pub body: Body,
}

/// What came out of an invocation.
#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
pub enum Body {
    /// How the database stands: the `status` and `check` commands.
    Status(Status),

    /// What the run did: the `run` command.
    Run(Run),

    /// The command failed.
    Error {
        /// The failure, in words.
        error: String,
    },
}

/// How a database stands against the migrations of this build.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Status {
    /// The database was never migrated: every migration is pending.
    NotMigrated {
        /// The migrations that are not applied, in order.
        pending: Vec<PendingMigration>,
    },

    /// Some migrations are applied, and the rest are pending.
    Pending {
        /// How many migrations are applied.
        applied: usize,

        /// The migrations that are not applied, in order.
        pending: Vec<PendingMigration>,

        /// Whether a holder has the exclusive migrations lock.
        migration_in_progress: bool,
    },

    /// Every migration is applied, and the database has no other.
    UpToDate {
        /// How many migrations are applied.
        applied: usize,

        /// Whether a holder has the exclusive migrations lock.
        migration_in_progress: bool,
    },

    /// A migration was started and never finished.
    Dirty {
        /// The version of the migration.
        version: u32,

        /// Whether a holder has the exclusive migrations lock: a run with
        /// unmanaged transactions shows as dirty while it is in progress.
        migration_in_progress: bool,
    },

    /// The migrations ledger conflicts with the migrations of this build.
    LedgerMismatch {
        /// The conflict, in words.
        detail: String,

        /// Whether a holder has the exclusive migrations lock.
        migration_in_progress: bool,
    },

    /// The database does not meet an assertion of the migrations engine:
    /// neither the ledger nor a lock was looked for.
    AssertionViolated {
        /// What is wrong, in the assertion's words.
        message: String,
    },
}

/// A migration that is not applied.
#[derive(Debug, serde::Serialize)]
pub struct PendingMigration {
    /// The version of the migration.
    pub version: u32,

    /// The description of the migration.
    pub description: String,
}

/// What a run did.
#[derive(Debug, serde::Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Run {
    /// No migration was pending; no lock was taken.
    NothingPending,

    /// The pending migrations were applied.
    Applied {
        /// How many migrations were applied.
        migrations: usize,
    },

    /// The exclusive migrations lock was refused; nothing was applied.
    LockRefused {
        /// What refused it.
        reason: LockRefusedReason,
    },

    /// A migration was started and never finished; nothing was applied.
    Dirty {
        /// The version of the migration.
        version: u32,
    },

    /// The migrations ledger conflicts with the migrations of this build;
    /// nothing was applied.
    LedgerMismatch {
        /// The conflict, in words.
        detail: String,
    },

    /// The database does not meet an assertion of the migrations engine;
    /// nothing was applied.
    AssertionViolated {
        /// What is wrong, in the assertion's words.
        message: String,
    },
}

/// What refused the exclusive migrations lock.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LockRefusedReason {
    /// Another holder has the exclusive migrations lock.
    MigrationInProgress,

    /// Other holders have shared migrations locks.
    DatabaseInUse,
}

/// The exit codes of the tool. `2` is left to the command line parser,
/// which exits with it on a usage error.
#[derive(Debug)]
pub enum Exit {
    /// `status`: the status was reported; `check`: the database is
    /// migrated exactly to this build's migrations; `run`: the pending
    /// migrations were applied, or none was pending.
    Done,

    /// The command failed.
    Error,

    /// `check`: migrations are pending, or the database was never
    /// migrated.
    Pending,

    /// `check`: a migration is in progress; `run`: the exclusive
    /// migrations lock was refused.
    Locked,

    /// A migration was started and never finished.
    Dirty,

    /// The migrations ledger conflicts with the migrations of this build.
    LedgerMismatch,

    /// `check` and `run`: the database does not meet an assertion of the
    /// migrations engine.
    AssertionViolated,
}

impl Exit {
    /// The code the process exits with.
    pub fn code(&self) -> u8 {
        match self {
            Self::Done => 0,
            Self::Error => 1,
            Self::Pending => 3,
            Self::Locked => 4,
            Self::Dirty => 5,
            Self::LedgerMismatch => 6,
            Self::AssertionViolated => 7,
        }
    }
}

impl From<Exit> for std::process::ExitCode {
    fn from(exit: Exit) -> Self {
        Self::from(exit.code())
    }
}

/// The exit code of the `check` command over `status`.
pub fn check_exit(status: &Status) -> Exit {
    match status {
        Status::AssertionViolated { .. } => Exit::AssertionViolated,
        Status::LedgerMismatch { .. } => Exit::LedgerMismatch,
        Status::Pending {
            migration_in_progress: true,
            ..
        }
        | Status::UpToDate {
            migration_in_progress: true,
            ..
        }
        | Status::Dirty {
            migration_in_progress: true,
            ..
        } => Exit::Locked,
        Status::Dirty {
            migration_in_progress: false,
            ..
        } => Exit::Dirty,
        Status::NotMigrated { .. }
        | Status::Pending {
            migration_in_progress: false,
            ..
        } => Exit::Pending,
        Status::UpToDate {
            migration_in_progress: false,
            ..
        } => Exit::Done,
    }
}

/// The exit code of the `run` command over what it did.
pub fn run_exit(run: &Run) -> Exit {
    match run {
        Run::NothingPending | Run::Applied { .. } => Exit::Done,
        Run::LockRefused { .. } => Exit::Locked,
        Run::Dirty { .. } => Exit::Dirty,
        Run::LedgerMismatch { .. } => Exit::LedgerMismatch,
        Run::AssertionViolated { .. } => Exit::AssertionViolated,
    }
}

/// Write `report` to the standard output as one line of JSON and turn
/// `exit` into the exit code; a report that could not be written exits
/// with [`Exit::Error`].
pub fn write(report: &Report, exit: Exit) -> std::process::ExitCode {
    let line = match serde_json::to_string(report) {
        Ok(line) => line,
        Err(error) => {
            tracing::error!(%error, "the report could not be serialized");
            return Exit::Error.into();
        }
    };

    if let Err(error) = writeln!(std::io::stdout(), "{line}") {
        tracing::error!(%error, "the report could not be written to the standard output");
        return Exit::Error.into();
    }

    exit.into()
}

#[cfg(test)]
mod tests;
