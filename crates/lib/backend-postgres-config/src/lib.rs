//! Config for the main database: the Postgres store the runtime keeps its
//! state in.
//!
//! Its URL is `WAYMARK_MAIN_DATABASE_URL`, and the URL its migrations run
//! through is `WAYMARK_MAIN_MIGRATIONS_DATABASE_URL`; the unified
//! variables stand in for the ones that are not set, as
//! [`waymark_postgres_config::database_url`] has it.

#![warn(missing_docs)]

use waymark_postgres_config::database_url;
use waymark_secret_string::SecretString;

mod defaults_for;
pub mod migrate_only;
mod profiles;
pub mod read_write_migrate;
pub mod read_write_only;

pub use self::defaults_for::DefaultsFor;
pub use self::migrate_only::MigrateOnly;
pub use self::read_write_migrate::ReadWriteMigrate;
pub use self::read_write_only::ReadWriteOnly;

/// The main database's own URL variable.
const DATABASE_URL_VARIABLE: &str = "WAYMARK_MAIN_DATABASE_URL";

/// The main database's own migrations URL variable.
const MIGRATIONS_DATABASE_URL_VARIABLE: &str = "WAYMARK_MAIN_MIGRATIONS_DATABASE_URL";

/// The error of telling one of the main database's URLs from the
/// environment.
type UrlFromEnvError = database_url::FromEnvError;

/// The main database's URL: `WAYMARK_MAIN_DATABASE_URL`, or the unified
/// `WAYMARK_DATABASE_URL` when it is not set.
fn url_from_env() -> Result<SecretString, UrlFromEnvError> {
    database_url::from_env(DATABASE_URL_VARIABLE)
}

/// The URL the main database's migrations run through: the first that is
/// set of `WAYMARK_MAIN_MIGRATIONS_DATABASE_URL`,
/// `WAYMARK_MAIN_DATABASE_URL` and the unified
/// `WAYMARK_MIGRATIONS_DATABASE_URL` and `WAYMARK_DATABASE_URL`.
fn migrations_url_from_env() -> Result<SecretString, UrlFromEnvError> {
    database_url::migrations_from_env(MIGRATIONS_DATABASE_URL_VARIABLE, DATABASE_URL_VARIABLE)
}

#[cfg(test)]
mod tests;
