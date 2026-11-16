use waymark_vm_runtime_promise_core::PromiseStateId;

use crate::PromiseStateHandle;

/// A value for a runtime that supports suspending.
pub trait Suspendable {
    /// Construct a promisable value from a pending value.
    fn from_pending(promise_state_handle: PromiseStateHandle) -> Self;

    /// View the value as a promise state id.
    fn as_pending(&self) -> Option<PromiseStateId>;
}

/// A value type that supports promises.
///
/// Wraps a "resolved" value type with the possibilities of either it being
/// immediately available (i.e. resolved) or being in a placeholder
/// state waiting for a wrapped value to be resolved with.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Promisable: Suspendable + waymark_vm_runtime_promise_core::Resolvable {}
