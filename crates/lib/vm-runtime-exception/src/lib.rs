//! The exception traits of the VM runtime.
//!
//! The VM has no exception representation of its own. A raised exception
//! is whatever type the instruction set's spec names, and the `Exception`
//! variant of a value holds whatever type the value's flavor names; the two
//! need not be the same type, and a language may have no exception values
//! at all and still raise and catch. These traits are how the runtime and
//! the interpreters work with either without knowing its shape: how a
//! raised exception matches a handler's pattern, and how an exception
//! crosses between the raised domain and the value domain.

#![warn(missing_docs)]

/// The type of the exception match pattern: the value an exception handler
/// keeps, and what a raised exception is matched against.
///
/// The pattern is whatever the language compiles an `except` clause to -
/// for Python, a class name. Everything that carries a raised exception
/// and its handler stack needs only this; matching itself is
/// [`Match`]. Implemented by the raised exception type.
pub trait HasMatchPattern {
    /// The exception match pattern type: what an exception handler keeps,
    /// and what a raised exception of the implementing type is matched
    /// against.
    type Pattern;
}

/// Match a raised exception against a handler's pattern.
///
/// The match is the language's rule. Implemented by the raised exception
/// type; required only where the unwind decides which handler catches.
pub trait Match: HasMatchPattern {
    /// Whether a handler listing `pattern` catches this exception.
    fn matches(&self, pattern: &Self::Pattern) -> bool;
}

/// The runtime exception pattern type of a raised exception type: what its
/// handlers hold and it matches against.
pub type MatchPatternOf<RaisedException> = <RaisedException as HasMatchPattern>::Pattern;

/// A language without raised exceptions has no pattern, trivially.
impl HasMatchPattern for core::convert::Infallible {
    type Pattern = core::convert::Infallible;
}

/// A language without raised exceptions has nothing to match, ever.
impl Match for core::convert::Infallible {
    fn matches(&self, _pattern: &Self::Pattern) -> bool {
        match *self {}
    }
}

/// Turn a value into a raised exception: what the `Raise` instruction does
/// to the register it names.
pub trait ValueToRaisedException<RaisedException>: Sized {
    /// Why the value cannot be raised.
    type Error;

    /// Raise this value.
    fn into_raised(self) -> Result<RaisedException, Self::Error>;
}

/// Capture a raised exception as a value: what a handler does with the
/// exception it caught when it has a destination register.
pub trait RaisedExceptionToValue<RaisedException>: Sized {
    /// Why the raised exception cannot be held as a value.
    type Error;

    /// Capture the raised exception.
    fn from_raised(raised: RaisedException) -> Result<Self, Self::Error>;
}

/// The error of capturing a raised exception in a language that has no
/// exception values: such a language catches by matching alone, and its
/// compiler never emits a handler with a destination register.
#[derive(Debug, thiserror::Error)]
#[error("this language has no exception values")]
pub struct NoExceptionValuesError;

/// An uninhabited exception value can never be raised, trivially.
impl<RaisedException> ValueToRaisedException<RaisedException> for core::convert::Infallible {
    type Error = core::convert::Infallible;

    fn into_raised(self) -> Result<RaisedException, Self::Error> {
        match self {}
    }
}

/// An uninhabited exception value can never capture anything.
impl<RaisedException> RaisedExceptionToValue<RaisedException> for core::convert::Infallible {
    type Error = NoExceptionValuesError;

    fn from_raised(_raised: RaisedException) -> Result<Self, Self::Error> {
        Err(NoExceptionValuesError)
    }
}
