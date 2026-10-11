//! Config for the main database: the Postgres store the runtime keeps its
//! state in, `WAYMARK_DATABASE_URL`.

#![warn(missing_docs)]

use waymark_secret_string::SecretString;

pub mod migrate_only;
pub mod read_write_migrate;
pub mod read_write_only;

pub use self::migrate_only::MigrateOnly;
pub use self::read_write_migrate::ReadWriteMigrate;
pub use self::read_write_only::ReadWriteOnly;

/// The error of reading a required URL from the environment.
type MustUrlFromEnvError = envfury::Error<envfury::MustError<std::convert::Infallible>>;

/// The error of reading a URL with a default from the environment.
type UrlFromEnvError = envfury::Error<envfury::ValueError<std::convert::Infallible>>;

/// The error of reading a number with a default from the environment.
type IntFromEnvError = envfury::Error<envfury::OrParseError<std::num::ParseIntError>>;

/// The database URL: `WAYMARK_DATABASE_URL`, required.
fn url_from_env() -> Result<SecretString, MustUrlFromEnvError> {
    envfury::must("WAYMARK_DATABASE_URL")
}

/// The URL the migrations run through:
/// `WAYMARK_MIGRATIONS_DATABASE_URL`, or `url` when it is not set.
fn migrations_url_from_env_with_default(
    url: &SecretString,
) -> Result<SecretString, UrlFromEnvError> {
    envfury::or_else("WAYMARK_MIGRATIONS_DATABASE_URL", || url.clone())
}

#[cfg(test)]
mod tests;
