//! Tests of tuples of assertions.

use super::{CheckError, EmptySlot};
use crate::tests::ready;
use crate::{Assertion, Outcome};

/// The connection the test assertions are checked through: it keeps which
/// of them were checked, in order.
type TestConnection = Vec<&'static str>;

/// The check error of the first test assertion.
#[derive(Debug, thiserror::Error)]
#[error("first error")]
struct FirstError;

/// The check error of the second test assertion.
#[derive(Debug, thiserror::Error)]
#[error("second error")]
struct SecondError;

/// What a test assertion's check comes to.
#[derive(Debug)]
enum Check {
    /// It holds.
    Holds,

    /// It is violated, with this message.
    Violated(&'static str),

    /// It cannot be checked.
    Fails,
}

/// A test assertion with a check error of its own type. It is not `Debug`:
/// the tuple's error does not ask that of an assertion.
struct First(Check);

/// Another test assertion, with another check error type.
struct Second(Check);

impl Assertion for First {
    type Connection = TestConnection;

    type CheckError = FirstError;

    async fn check(&self, connection: &mut TestConnection) -> Result<Outcome, FirstError> {
        connection.push("first");

        match self.0 {
            Check::Holds => Ok(Outcome::Holds),
            Check::Violated(message) => Ok(Outcome::Violated {
                message: message.to_owned(),
            }),
            Check::Fails => Err(FirstError),
        }
    }
}

impl Assertion for Second {
    type Connection = TestConnection;

    type CheckError = SecondError;

    async fn check(&self, connection: &mut TestConnection) -> Result<Outcome, SecondError> {
        connection.push("second");

        match self.0 {
            Check::Holds => Ok(Outcome::Holds),
            Check::Violated(message) => Ok(Outcome::Violated {
                message: message.to_owned(),
            }),
            Check::Fails => Err(SecondError),
        }
    }
}

/// The check error of a tuple of the two test assertions.
type PairCheckError =
    CheckError<First, Second, EmptySlot, EmptySlot, EmptySlot, EmptySlot, EmptySlot, EmptySlot>;

/// Check the pair of test assertions; give the outcome and what was
/// checked.
fn check_pair(first: Check, second: Check) -> (Result<Outcome, PairCheckError>, TestConnection) {
    let mut connection = TestConnection::new();

    let checked = ready((First(first), Second(second)).check(&mut connection));

    (checked, connection)
}

#[test]
fn tuple_holds_when_each_element_does() {
    let (checked, connection) = check_pair(Check::Holds, Check::Holds);

    assert!(matches!(checked, Ok(Outcome::Holds)));
    assert_eq!(connection, ["first", "second"]);
}

#[test]
fn first_violated_element_is_what_the_check_comes_to() {
    let (checked, connection) = check_pair(Check::Violated("first is wrong"), Check::Fails);

    assert!(matches!(checked, Ok(Outcome::Violated { message }) if message == "first is wrong"));
    assert_eq!(connection, ["first"]);

    let (checked, connection) = check_pair(Check::Holds, Check::Violated("second is wrong"));

    assert!(matches!(checked, Ok(Outcome::Violated { message }) if message == "second is wrong"));
    assert_eq!(connection, ["first", "second"]);
}

#[test]
fn error_is_the_position_of_the_element_that_could_not_be_checked() {
    let (checked, connection) = check_pair(Check::Fails, Check::Holds);

    assert!(matches!(checked, Err(CheckError::Element0(FirstError))));
    assert_eq!(connection, ["first"]);

    let (checked, connection) = check_pair(Check::Holds, Check::Fails);

    let Err(error) = checked else {
        panic!("the second element cannot be checked");
    };
    assert!(matches!(error, CheckError::Element1(SecondError)));
    assert_eq!(error.to_string(), "second error");
    // The label is the assertion's type as the compiler spells it, which
    // is not pinned here beyond its last part.
    assert!(format!("{error:?}").ends_with("::Second(SecondError)"));
    assert!(format!("{error:#?}").ends_with("::Second(\n    SecondError,\n)"));
    assert!(core::error::Error::source(&error).is_some_and(|source| source.is::<SecondError>()));
    assert_eq!(connection, ["first", "second"]);
}

#[test]
fn tuple_of_one_element_is_an_assertion() {
    let mut connection = TestConnection::new();

    let checked = ready((First(Check::Holds),).check(&mut connection));

    assert!(matches!(checked, Ok(Outcome::Holds)));
    assert_eq!(connection, ["first"]);
}

#[test]
fn tuple_of_a_tuple_is_an_assertion() {
    let mut connection = TestConnection::new();
    let nested = (
        (First(Check::Holds),),
        Second(Check::Violated("second is wrong")),
    );

    let checked = ready(nested.check(&mut connection));

    assert!(matches!(checked, Ok(Outcome::Violated { message }) if message == "second is wrong"));
    assert_eq!(connection, ["first", "second"]);
}
