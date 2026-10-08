use super::*;

fn assert_send_sync<T: Send + Sync>() {}

// The handler is meant to be driven from a spawned task and shared, so it
// must stay `Send + Sync` regardless of the `ReadyValue` type parameter —
// which it never owns. Assert that with a deliberately `!Send + !Sync`
// `ReadyValue` (`Rc`), the handler remains both.
#[test]
fn handler_is_send_sync_independent_of_ready_value() {
    assert_send_sync::<EffectHandler<(), (), std::rc::Rc<()>, ()>>();
}

// The same for the `RaisedException` type parameter, which the handler
// never owns either.
#[test]
fn handler_is_send_sync_independent_of_raised_exception() {
    assert_send_sync::<EffectHandler<(), (), (), std::rc::Rc<()>>>();
}
