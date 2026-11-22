//! Tests of the check of a database's assertions, over mocked assertions.

use waymark_migrations_engine_assertion::Outcome;

use super::{Error, check_migrations_engine_assertions};
use crate::test_helpers::{TestAssertion, TestAssertionError, TestConnection};

#[tokio::test]
async fn assertions_that_hold_are_checked_in_the_order_of_the_tuple() {
    let mut connection = TestConnection::default();
    let mut assertions = (
        TestAssertion::checked_once("the first", || Ok(Outcome::Holds)),
        TestAssertion::checked_once("the second", || Ok(Outcome::Holds)),
    );

    let checked = check_migrations_engine_assertions(&mut connection, &assertions).await;

    assert!(checked.is_ok());
    assert_eq!(
        connection.events.so_far(),
        ["check the first", "check the second"]
    );

    assertions.0.operations.checkpoint();
    assertions.1.operations.checkpoint();
}

#[tokio::test]
async fn no_assertions_hold() {
    let mut connection = TestConnection::default();

    let checked = check_migrations_engine_assertions(&mut connection, &TestAssertion::NONE).await;

    assert!(checked.is_ok());
    assert!(connection.events.so_far().is_empty());
}

#[tokio::test]
async fn one_assertion_alone_is_checked() {
    let mut connection = TestConnection::default();
    let mut assertion = TestAssertion::checked_once("the only one", || Ok(Outcome::Holds));

    let checked = check_migrations_engine_assertions(&mut connection, &assertion).await;

    assert!(checked.is_ok());
    assert_eq!(connection.events.so_far(), ["check the only one"]);

    assertion.operations.checkpoint();
}

#[tokio::test]
async fn violated_assertion_is_reported_and_the_ones_after_it_are_not_checked() {
    let mut connection = TestConnection::default();
    let mut assertions = (
        TestAssertion::checked_once("the first", || Ok(Outcome::Holds)),
        TestAssertion::checked_once("the second", || {
            TestAssertion::violated("the second is wrong")
        }),
        // The mock fails the test on any call.
        TestAssertion::untouched(),
    );

    let checked = check_migrations_engine_assertions(&mut connection, &assertions).await;

    let Err(error @ Error::Violated { .. }) = checked else {
        panic!("the second assertion is violated");
    };
    assert_eq!(
        error.to_string(),
        "the database does not meet an assertion: the second is wrong"
    );

    assertions.0.operations.checkpoint();
    assertions.1.operations.checkpoint();
    assertions.2.operations.checkpoint();
}

#[tokio::test]
async fn assertion_that_cannot_be_checked_is_reported_with_its_error() {
    let mut connection = TestConnection::default();
    let mut assertions = (
        TestAssertion::checked_once("the first", || Err(TestAssertionError)),
        TestAssertion::untouched(),
    );

    let checked = check_migrations_engine_assertions(&mut connection, &assertions).await;

    let Err(
        error @ Error::Check(waymark_migrations_engine_assertion::tuple::CheckError::Element0(
            TestAssertionError,
        )),
    ) = checked
    else {
        panic!("the first assertion's check fails");
    };
    assert_eq!(
        error.to_string(),
        "checking the assertions: test assertion error"
    );

    assertions.0.operations.checkpoint();
    assertions.1.operations.checkpoint();
}
