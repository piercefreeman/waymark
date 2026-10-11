//! Config for the observability store: the backend-neutral database URL,
//! dispatched to a backend by its scheme.
//!
//! By default the observability store lives in the main database
//! (`WAYMARK_DATABASE_URL`) — same URL, namespace-separated into its own
//! schema and accessed through its own pools. Pointing
//! `WAYMARK_OBSERVABILITY_DATABASE_URL` elsewhere moves it to a separate
//! server; `WAYMARK_OBSERVABILITY_READ_DATABASE_URL` is where the reads
//! go — the same database by default, or a replica of it.

#![warn(missing_docs)]

use waymark_secret_string::SecretString;

pub mod read_only;
pub mod read_write;
pub mod write_only;

pub use self::read_only::ReadOnly;
pub use self::read_write::ReadWrite;
pub use self::write_only::WriteOnly;

/// The error of reading one of the store's URLs from the environment.
type UrlFromEnvError = envfury::Error<envfury::ValueError<std::convert::Infallible>>;

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

/// The store's URL: `WAYMARK_OBSERVABILITY_DATABASE_URL`, or
/// `default_database_url` when it is not set.
fn url_from_env_with_default(
    default_database_url: &SecretString,
) -> Result<SecretString, UrlFromEnvError> {
    envfury::or_else("WAYMARK_OBSERVABILITY_DATABASE_URL", || {
        default_database_url.clone()
    })
}

/// The URL the store is read through:
/// `WAYMARK_OBSERVABILITY_READ_DATABASE_URL`, or `url` when it is not set.
fn read_url_from_env_with_default(url: &SecretString) -> Result<SecretString, UrlFromEnvError> {
    envfury::or_else("WAYMARK_OBSERVABILITY_READ_DATABASE_URL", || url.clone())
}

#[cfg(test)]
mod tests;
