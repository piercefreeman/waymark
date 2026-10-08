//! The promise-settled hook and the types it is declared over.

use typle::typle;
use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Carries the type of the values a hook observes in promise settlements.
pub trait HasValue {
    /// The type of the values promises are resolved with.
    type Value;
}

/// Carries the type of the raised exceptions a hook observes in promise
/// settlements.
pub trait HasRaisedException {
    /// The type of the raised exceptions promises are rejected with.
    type RaisedException;
}

/// Observes the promise settlements the driver receives.
pub trait PromiseSettled: HasValue + HasRaisedException {
    /// A promise settlement has reached the driver.
    ///
    /// Called with the settlement as received, before it is applied to the
    /// runtime.
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value, Self::RaisedException>,
    );
}

/// A tuple of hooks is declared over the value type its components agree
/// on.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Value> HasValue for T
where
    T<_>: HasValue<Value = Value>,
{
    type Value = Value;
}

/// A tuple of hooks is declared over the raised exception type its
/// components agree on.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, RaisedException> HasRaisedException for T
where
    T<_>: HasRaisedException<RaisedException = RaisedException>,
{
    type RaisedException = RaisedException;
}

/// A tuple of hooks observes as each of its components in turn, first to
/// last.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Value, RaisedException> PromiseSettled for T
where
    T<_>: PromiseSettled<Value = Value, RaisedException = RaisedException>,
{
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value, Self::RaisedException>,
    ) {
        for typle_index!(i) in 0..T::LEN {
            self[[i]].promise_settled(promise_state_id, resolution);
        }
    }
}

/// An optional hook is declared over its hook's value type.
impl<Hooks> HasValue for Option<Hooks>
where
    Hooks: HasValue,
{
    type Value = Hooks::Value;
}

/// An optional hook is declared over its hook's raised exception type.
impl<Hooks> HasRaisedException for Option<Hooks>
where
    Hooks: HasRaisedException,
{
    type RaisedException = Hooks::RaisedException;
}

/// An optional hook observes when present and not at all when absent.
impl<Hooks> PromiseSettled for Option<Hooks>
where
    Hooks: PromiseSettled,
{
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value, Self::RaisedException>,
    ) {
        if let Some(hooks) = self {
            hooks.promise_settled(promise_state_id, resolution);
        }
    }
}
