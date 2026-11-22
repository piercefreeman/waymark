//! The check of a database's assertions, which every flow begins with.

use waymark_migrations_engine_assertion::Outcome;

/// Error of checking the assertions of a database.
#[derive(Debug, thiserror::Error)]
pub enum Error<CheckError> {
    /// The assertions could not be checked.
    #[error("checking the assertions: {0}")]
    Check(#[source] CheckError),

    /// The database does not meet an assertion.
    #[error("the database does not meet an assertion: {message}")]
    Violated {
        /// What is wrong, in the assertion's words.
        message: String,
    },
}

/// Shorthand for the [`Error`] of checking assertions of the type
/// `Assertion`.
pub type ErrorFor<Assertion> = Error<waymark_migrations_engine_assertion::CheckErrorFor<Assertion>>;

/// Check `assertions` through `connection`: one assertion, or a tuple of
/// them, which checks its elements in turn up to the first one that does
/// not hold or cannot be checked.
pub(crate) async fn check_migrations_engine_assertions<Assertion>(
    connection: &mut <Assertion as waymark_migrations_engine_assertion::Assertion>::Connection,
    assertions: &Assertion,
) -> Result<(), ErrorFor<Assertion>>
where
    Assertion: waymark_migrations_engine_assertion::Assertion,
{
    let checked = waymark_migrations_engine_assertion::Assertion::check(assertions, connection)
        .await
        .map_err(Error::Check)?;

    match checked {
        Outcome::Holds => Ok(()),
        Outcome::Violated { message } => Err(Error::Violated { message }),
    }
}

#[cfg(test)]
mod tests;
