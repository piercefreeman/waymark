//! Tuples of assertions: a tuple of one to eight assertions that are
//! checked through the same connection type is an assertion.
//!
//! The tuple checks its elements in turn, first to last, up to the first
//! one that does not hold or cannot be checked.

use typle::typle;

use crate::{Assertion, Outcome};

/// What fills the positions of [`CheckError`] that a shorter tuple does
/// not have: nothing is ever in one.
#[derive(Debug)]
pub enum EmptySlot {}

impl Assertion for EmptySlot {
    type Connection = ();

    type CheckError = core::convert::Infallible;

    async fn check(&self, _connection: &mut ()) -> Result<Outcome, Self::CheckError> {
        match *self {}
    }
}

/// Error of checking a tuple of assertions: one of its elements could not
/// be checked. The variant is the element's position.
///
/// It shows as the element's error does, and that error is its source.
#[typle(Tuple for 1..=8, never = EmptySlot)]
pub enum CheckError<T>
where
    T: Tuple,
    T<_>: Assertion,
{
    /// The assertion in this position could not be checked.
    Element = typle_variant!(i in ..T::MAX => <T<{i}> as Assertion>::CheckError),
}

// Written by hand: a derive would ask the assertions themselves to be
// `Debug`, and only their check errors are in here. An element's error
// shows under the type of its assertion, which says more than its position.
#[typle(Tuple for 8..=8, never = EmptySlot)]
impl<T> core::fmt::Debug for CheckError<T<{ ..T::MAX }>>
where
    T: Tuple,
    T<_>: Assertion<CheckError: core::fmt::Debug>,
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for typle_index!(i) in 0..T::LEN {
            if let Self::Element::<typle_ident!(i)>(error) = self {
                return formatter
                    .debug_tuple(core::any::type_name::<T<{ i }>>())
                    .field(error)
                    .finish();
            }
        }

        // Every variant is one of the positions above.
        Ok(())
    }
}

#[typle(Tuple for 8..=8, never = EmptySlot)]
impl<T> core::fmt::Display for CheckError<T<{ ..T::MAX }>>
where
    T: Tuple,
    T<_>: Assertion<CheckError: core::fmt::Display>,
{
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for typle_index!(i) in 0..T::LEN {
            if let Self::Element::<typle_ident!(i)>(error) = self {
                return core::fmt::Display::fmt(error, formatter);
            }
        }

        // Every variant is one of the positions above.
        Ok(())
    }
}

#[typle(Tuple for 8..=8, never = EmptySlot)]
impl<T> core::error::Error for CheckError<T<{ ..T::MAX }>>
where
    T: Tuple,
    T<_>: Assertion<CheckError: core::error::Error + 'static>,
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        for typle_index!(i) in 0..T::LEN {
            if let Self::Element::<typle_ident!(i)>(error) = self {
                return Some(error);
            }
        }

        // Every variant is one of the positions above.
        None
    }
}

/// A tuple of assertions holds when each of its elements does. The first
/// element that does not hold, or cannot be checked, is what the check
/// comes to; the ones after it are not checked.
#[typle(Tuple for 1..=8, never = EmptySlot)]
impl<T, Connection> Assertion for T
where
    T: Tuple,
    T<_>: Assertion<Connection = Connection>,
    T<_>: Sync,
    Connection: Send,
{
    type Connection = Connection;

    type CheckError = CheckError<T<{ ..T::MAX }>>;

    async fn check(&self, connection: &mut Connection) -> Result<Outcome, Self::CheckError> {
        for typle_index!(i) in 0..T::LEN {
            let checked = self[[i]].check(connection).await;

            match checked {
                Ok(Outcome::Holds) => {}
                Ok(Outcome::Violated { message }) => {
                    return Ok(Outcome::Violated { message });
                }
                Err(error) => {
                    return Err(CheckError::Element::<typle_ident!(i)>(error));
                }
            }
        }

        Ok(Outcome::Holds)
    }
}

#[cfg(test)]
mod tests;
