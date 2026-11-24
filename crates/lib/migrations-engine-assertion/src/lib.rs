//! The assertion interface of the migrations engine: the trait an
//! assertion implements.
//!
//! An assertion is a condition a database has to meet before the
//! migrations engine does anything with it. The caller of the engine
//! supplies the assertions of a database beside its migrations.
//!
//! With the `tuples` feature a tuple of assertions is an assertion too:
//! see [`tuple`].

#![warn(missing_docs)]

#[cfg(feature = "tuples")]
pub mod tuple;

/// What a check came to.
#[derive(Debug)]
pub enum Outcome {
    /// The database meets the assertion.
    Holds,

    /// The database does not meet the assertion.
    Violated {
        /// What is wrong, in the assertion's words.
        message: String,
    },
}

/// A condition a database has to meet before the migrations engine does
/// anything with it.
pub trait Assertion {
    /// The connection type the assertion is checked through.
    type Connection;

    /// An error that can occur while checking.
    type CheckError;

    /// Check the assertion through `connection`. Nothing is written.
    fn check<'a>(
        &'a self,
        connection: &'a mut Self::Connection,
    ) -> impl Future<Output = Result<Outcome, Self::CheckError>> + Send + 'a;
}

/// The error of checking an assertion of the type `Assertion`.
pub type CheckErrorFor<Assertion> = <Assertion as crate::Assertion>::CheckError;

/// No assertions, for a database checked through a `Connection`: it always
/// holds.
pub struct NoAssertions<Connection> {
    /// The connection type the assertions would be checked through.
    pub connection_type: core::marker::PhantomData<fn(&mut Connection)>,
}

impl<Connection> core::fmt::Debug for NoAssertions<Connection> {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("NoAssertions").finish()
    }
}

impl<Connection> Assertion for NoAssertions<Connection> {
    type Connection = Connection;

    type CheckError = core::convert::Infallible;

    fn check<'a>(
        &'a self,
        _connection: &'a mut Connection,
    ) -> impl Future<Output = Result<Outcome, Self::CheckError>> + Send + 'a {
        core::future::ready(Ok(Outcome::Holds))
    }
}

#[cfg(test)]
mod tests;
