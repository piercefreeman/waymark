//! Where a database's URLs come from: the database's own variables, with
//! the unified variables behind them.
//!
//! The unified variables belong to no database: `WAYMARK_DATABASE_URL` is
//! what a database with no URL of its own is reached at, and
//! `WAYMARK_MIGRATIONS_DATABASE_URL` is what such a database is migrated
//! through.

use waymark_secret_string::SecretString;

/// The unified database URL variable.
const UNIFIED_DATABASE_URL_VARIABLE: &str = "WAYMARK_DATABASE_URL";

/// The unified migrations database URL variable.
const UNIFIED_MIGRATIONS_DATABASE_URL_VARIABLE: &str = "WAYMARK_MIGRATIONS_DATABASE_URL";

/// The error of reading one URL variable from the environment.
pub type ReadError = envfury::Error<envfury::ValueError<core::convert::Infallible>>;

/// Error returned by [`from_env`] and [`migrations_from_env`].
#[derive(Debug, thiserror::Error)]
pub enum FromEnvError {
    /// A variable could not be read.
    #[error("reading a database url variable: {0}")]
    Read(#[source] ReadError),

    /// None of the variables the URL can come from is set.
    #[error("none of {} is set", .variables.join(", "))]
    NotSet {
        /// The variables, in the order they were looked at.
        variables: Vec<&'static str>,
    },
}

/// The value of the first of `variables` that is set, in their order;
/// `None` when none is. The variables after it are not read.
fn first_set<Error>(
    variables: &[&'static str],
    mut read: impl FnMut(&'static str) -> Result<Option<SecretString>, Error>,
) -> Result<Option<SecretString>, Error> {
    for variable in variables {
        if let Some(url) = read(variable)? {
            return Ok(Some(url));
        }
    }

    Ok(None)
}

/// The first of `variables` that is set in the environment, in their
/// order.
fn first_set_from_env(variables: &[&'static str]) -> Result<SecretString, FromEnvError> {
    let url = first_set(variables, envfury::maybe).map_err(FromEnvError::Read)?;

    url.ok_or_else(|| FromEnvError::NotSet {
        variables: variables.to_vec(),
    })
}

/// The variables a database's URL comes from, in the order they are
/// looked at: the database's own, then the unified one.
fn database_url_variables(dedicated_variable: &'static str) -> [&'static str; 2] {
    [dedicated_variable, UNIFIED_DATABASE_URL_VARIABLE]
}

/// The variables a database's migrations URL comes from, in the order
/// they are looked at: the database's own migrations variable, then its
/// own URL variable — a database with a URL of its own is migrated there —
/// then the unified migrations variable, then the unified one.
fn migrations_database_url_variables(
    dedicated_migrations_variable: &'static str,
    dedicated_variable: &'static str,
) -> [&'static str; 4] {
    [
        dedicated_migrations_variable,
        dedicated_variable,
        UNIFIED_MIGRATIONS_DATABASE_URL_VARIABLE,
        UNIFIED_DATABASE_URL_VARIABLE,
    ]
}

/// The URL of the database whose own URL variable is `dedicated_variable`:
/// that variable, or `WAYMARK_DATABASE_URL` when it is not set.
pub fn from_env(dedicated_variable: &'static str) -> Result<SecretString, FromEnvError> {
    first_set_from_env(&database_url_variables(dedicated_variable))
}

/// The URL the database is migrated through, for the database whose own
/// variables are `dedicated_migrations_variable` and `dedicated_variable`:
/// the first that is set of `dedicated_migrations_variable`,
/// `dedicated_variable`, `WAYMARK_MIGRATIONS_DATABASE_URL` and
/// `WAYMARK_DATABASE_URL`.
pub fn migrations_from_env(
    dedicated_migrations_variable: &'static str,
    dedicated_variable: &'static str,
) -> Result<SecretString, FromEnvError> {
    first_set_from_env(&migrations_database_url_variables(
        dedicated_migrations_variable,
        dedicated_variable,
    ))
}

#[cfg(test)]
mod tests;
