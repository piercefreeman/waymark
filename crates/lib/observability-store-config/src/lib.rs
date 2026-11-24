//! Config for the observability store: the backend-neutral database URL,
//! dispatched to a backend by its scheme.
//!
//! The store's URL is `WAYMARK_OBSERVABILITY_DATABASE_URL`;
//! `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is where the reads go — the
//! same database by default, or a replica of it — and
//! `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL` is where the migrations
//! run, on a connection of their own. The unified variables stand in for
//! the ones that are not set, as [`waymark_postgres_config::database_url`]
//! has it: the store takes nothing from another database's config.

#![warn(missing_docs)]

use waymark_postgres_config::database_url;
use waymark_secret_string::SecretString;

pub mod migrate_only;
pub mod read_only;
pub mod read_write;
pub mod read_write_migrate;
pub mod write_only;

pub use self::migrate_only::MigrateOnly;
pub use self::read_only::ReadOnly;
pub use self::read_write::ReadWrite;
pub use self::read_write_migrate::ReadWriteMigrate;
pub use self::write_only::WriteOnly;

/// The store's own URL variable.
const DATABASE_URL_VARIABLE: &str = "WAYMARK_OBSERVABILITY_DATABASE_URL";

/// The store's own read URL variable.
const READ_DATABASE_URL_VARIABLE: &str = "WAYMARK_OBSERVABILITY_READ_DATABASE_URL";

/// The store's own migrations URL variable.
const MIGRATIONS_DATABASE_URL_VARIABLE: &str = "WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL";

/// The error of telling the store's URL or its migrations URL from the
/// environment.
type UrlFromEnvError = database_url::FromEnvError;

/// The error of reading the store's read URL from the environment.
type ReadUrlFromEnvError = database_url::ReadError;

/// The scheme of a URL; empty when it has none.
fn scheme(url: &SecretString) -> String {
    url.expose_secret()
        .split_once("://")
        .map(|(scheme, _)| scheme.to_owned())
        .unwrap_or_default()
}

/// A backend an observability store can be on.
enum Backend {
    /// Postgres.
    Postgres,
}

/// The backend a scheme names, if any.
fn backend(scheme: &str) -> Option<Backend> {
    // The backend is what must agree, not the spelling.
    match scheme {
        "postgres" | "postgresql" => Some(Backend::Postgres),
        _ => None,
    }
}

/// The store's URL: `WAYMARK_OBSERVABILITY_DATABASE_URL`, or the unified
/// `WAYMARK_DATABASE_URL` when it is not set.
fn url_from_env() -> Result<SecretString, UrlFromEnvError> {
    database_url::from_env(DATABASE_URL_VARIABLE)
}

/// The URL the store is read through:
/// `WAYMARK_OBSERVABILITY_READ_DATABASE_URL`, or `url`, the store's URL,
/// when it is not set.
fn read_url_from_env_with_default(url: &SecretString) -> Result<SecretString, ReadUrlFromEnvError> {
    envfury::or_else(READ_DATABASE_URL_VARIABLE, || url.clone())
}

/// The URL the store is migrated through: the first that is set of
/// `WAYMARK_OBSERVABILITY_MIGRATIONS_DATABASE_URL`,
/// `WAYMARK_OBSERVABILITY_DATABASE_URL` and the unified
/// `WAYMARK_MIGRATIONS_DATABASE_URL` and `WAYMARK_DATABASE_URL`.
fn migrations_url_from_env() -> Result<SecretString, UrlFromEnvError> {
    database_url::migrations_from_env(MIGRATIONS_DATABASE_URL_VARIABLE, DATABASE_URL_VARIABLE)
}

#[cfg(test)]
mod tests;
