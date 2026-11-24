//! Tests of the assertion interface's own types.

use super::{Assertion as _, NoAssertions, Outcome};

/// The output of `future`, which has to be ready at its first poll.
pub fn ready<F>(future: F) -> F::Output
where
    F: Future,
{
    let mut future = core::pin::pin!(future);
    let mut context = core::task::Context::from_waker(core::task::Waker::noop());

    match future.as_mut().poll(&mut context) {
        core::task::Poll::Ready(output) => output,
        core::task::Poll::Pending => panic!("the future is ready at its first poll"),
    }
}

#[test]
fn no_assertions_always_hold() {
    let mut connection = Vec::<&str>::new();

    let no_assertions = NoAssertions {
        connection_type: core::marker::PhantomData,
    };

    let checked = ready(no_assertions.check(&mut connection));

    assert!(matches!(checked, Ok(Outcome::Holds)));
    assert!(connection.is_empty());
}
