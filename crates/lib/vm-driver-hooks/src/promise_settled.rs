//! The promise-settled hook and the value type it is declared over.

use typle::typle;
use waymark_vm_driver_core::PromiseResolution;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Carries the type of the values a hook observes in promise settlements.
pub trait HasValue {
    /// The type of the values promises are settled with.
    type Value;
}

/// Observes the promise settlements the driver receives.
pub trait PromiseSettled: HasValue {
    /// A promise settlement has reached the driver.
    ///
    /// Called with the settlement as received, before it is applied to the
    /// runtime.
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value>,
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

/// A tuple of hooks observes as each of its components in turn, first to
/// last.
#[typle(Tuple for 1..=8)]
impl<T: Tuple, Value> PromiseSettled for T
where
    T<_>: PromiseSettled<Value = Value>,
{
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value>,
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

/// An optional hook observes when present and not at all when absent.
impl<Hooks> PromiseSettled for Option<Hooks>
where
    Hooks: PromiseSettled,
{
    fn promise_settled(
        &self,
        promise_state_id: PromiseStateId,
        resolution: &PromiseResolution<Self::Value>,
    ) {
        if let Some(hooks) = self {
            hooks.promise_settled(promise_state_id, resolution);
        }
    }
}
